#Copyright 2022 Nathan Harwood
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

from __future__ import annotations
import asyncio

import audiomodule.audiomodule as am

AE_OUTPUT_TIME = "output_time"
""" Reporting the maximum buffer time over output modules. """

AE_BUFFER_TIME = "buffer_time"
""" Reporting the buffer time for a buffer. """

AE_ERROR = "error"
""" An error has occurred in the audio engine. """

AE_INFO = "info"
""" Some useful information is available from the audio engine. """

class SequenceController(list[am.AudioModule]):
    """ Ensures modules produce output in the correct sequence. 
    
    Handles feedback (cycles) in the module graph using recursion. """

    def __init__(self):
        super().__init__()
        self._next_controller: __class__ = None
        self._prev_controller: __class__ = None
        self._current_output_time = 0.0

    def connect(self, next, root):
        self._root_controller = root
        self._next_controller = next
        next._prev_controller = self

    async def next_chunk(self,current_depth=0,depth_limit=None):
        if current_depth == depth_limit:
            raise Exception("Sequence controller has exceeded the depth limit.")
        self._current_output_time = 0.0
        # each module needs to advance by one chunk of data
        for module in self:
            mod_status = await module.next_chunk()
            while mod_status != am.AM_CONTINUE:
                # bail out if modules can no longer advance by a chunk of data
                if mod_status == am.AM_COMPLETED or mod_status == am.AM_ERROR:
                    return mod_status
                # if the module needs more input then ask the previous controller's
                # modules to advance by one chunk of data
                elif mod_status == am.AM_INPUT_REQUIRED:
                    if self._prev_controller:
                        # keep asking until the module can advance by one chunk
                        while mod_status == am.AM_INPUT_REQUIRED:
                            prev_status = await self._prev_controller.next_chunk(current_depth+1,depth_limit=depth_limit)
                            if prev_status == am.AM_COMPLETED or prev_status == am.AM_ERROR:
                                return prev_status
                            mod_status = await module.next_chunk()
                        if mod_status == am.AM_COMPLETED or mod_status == am.AM_ERROR:
                            return mod_status
                        if mod_status == am.AM_CONTINUE:
                            break
                    else:
                        raise Exception("Input is required from the root controller.")
                elif mod_status == am.AM_CYCLIC_UNDERRUN:
                    cyclic_status = await self._root_controller._prev_controller.next_chunk(1,depth_limit=current_depth)
                    if cyclic_status == am.AM_COMPLETED or cyclic_status == am.AM_ERROR:
                        return cyclic_status
                    mod_status = await module.next_chunk()
            if module.get_num_inputs()>0:
                self._current_output_time = max(
                    [self._current_output_time, module.get_in_buf().get_time()])
        return am.AM_CONTINUE

    def get_current_output_time(self):
        return self._current_output_time


class AudioEngine():
    """ Manage and control a set of modules. """
    
    def __init__(self):
        self._init()

    def _init(self):
        self._source_modules: list[am.AudioModule] = []
        self._internal_modules: list[am.AudioModule] = []
        self._sink_modules: list[am.AudioModule] = []
        self._sources_done = False
        self._running = False
        self._stopping = False
        self._opened = False
        self._root_controller = None

    def _configure_controllers(self):
        self._root_controller = SequenceController()
        modules_done = set([])
        sequence=0
        for module in self._sink_modules:
            self._root_controller.append(module)
            module.set_sequence(sequence)
            modules_done.add(module)
        next_modules = set([])
        for module in self._sink_modules:
            for i in range(module.get_num_inputs()):
                x = module.get_in_modules(i)
                if x != None and x[0] not in modules_done:
                    next_modules.add(x[0])
        current_modules = next_modules
        current_controller = self._root_controller
        while len(current_modules) > 0:
            next_controller = SequenceController()
            next_controller.connect(current_controller,self._root_controller)
            next_modules = set([])
            sequence+=1
            for module in current_modules:
                next_controller.append(module)
                module.set_sequence(sequence)
                modules_done.add(module)
            for module in current_modules:
                for i in range(module.get_num_inputs()):
                    x = module.get_in_modules(i)
                    if x != None and x[0] not in modules_done:
                        next_modules.add(x[0])
            current_modules = next_modules
            current_controller = next_controller
        return current_controller

    def _source_module(self, module: am.AudioModule) -> am.AudioModule:
        self._source_modules.append(module)
        return module

    def _sink_module(self, module: am.AudioModule) -> am.AudioModule:
        self._sink_modules.append(module)
        return module

    def module(self, module: am.AudioModule) -> am.AudioModule:
        """Enlist a module into the audio engine. 
        
        Returns the module enlisted. *Must not* be called while the
        audio engine is running.
        """

        if module.get_num_inputs() == 0:
            mod = self._source_module(module)
        elif module.get_num_outputs() == 0:
            mod = self._sink_module(module)
        else:
            self._internal_modules.append(module)
            mod = module
        return mod

    def delete_module(self, module: am.AudioModule):
        """Delete a module from the audio engine.
        
        *Must not* be called while the audio engine is running.
        """

        if module.get_num_inputs() == 0:
            self._source_modules = [
                x for x in self._source_modules if x != module]
        elif module.get_num_outputs() == 0:
            self._sink_modules = [x for x in self._sink_modules if x != module]
        else:
            self._internal_modules = [
                x for x in self._internal_modules if x != module]

    def reconf_module(self, module: am.AudioModule):
        """Reconfigure the module.
        
        Call when the module has changed its number of inputs or outputs."""

        self._source_modules = [
                x for x in self._source_modules if x != module]
        self._sink_modules = [x for x in self._sink_modules if x != module]
        self._internal_modules = [
                x for x in self._internal_modules if x != module]
        self.module(module)

    def delete_all_modules(self):
        """Closes all modules and re-inits the audio engine.
        
        *Must not* be called while the audio engine is currently running.
        """
        if self._opened:
            self.close_all()
        self._init()

    async def _send_to_observer(self,observer,msg,data,msg_id):
        observer.put((msg,data,msg_id))

    async def _run(self, observer):
        """Run all audio modules."""

        self._configure_controllers()
        free_runs=0
        while not self._sources_done and not self._stopping:
            # the root controller contains all sink modules
            status = await self._root_controller.next_chunk()
            observer.put((AE_OUTPUT_TIME, 
                self._root_controller.get_current_output_time(), None))
            if status == am.AM_ERROR:
                observer.put((AE_ERROR,"A module error occured and audio processing has stopped.",None))
                break
            elif status == am.AM_COMPLETED:
                observer.put((AE_INFO,"A source module has no more data to produce and audio processing has stopped.",None))
                break
            requires_data = False
            for module in self._sink_modules:
                requires_data |= module.requires_data
            if requires_data:
                free_runs+=1
            send_counts=0
            while (not requires_data and not self._stopping) or (free_runs>10):
                await asyncio.sleep(0.01)
                if free_runs==10:
                    send_counts=25
                free_runs=0
                if send_counts==0 or send_counts>25:
                    if send_counts==0:
                        send_counts=1
                    else:
                        send_counts+=1
                    for module in self._source_modules:
                        for out_idx in range(module.get_num_outputs()):
                            buf = module.get_out_buf(out_idx)
                            await self._send_to_observer(observer,AE_BUFFER_TIME,((module.mod_id,"out",out_idx),
                                                            buf.get_time()-buf.get_delayed_time()),None)
                    for module in self._internal_modules:
                        for out_idx in range(module.get_num_outputs()):
                            buf = module.get_out_buf(out_idx)
                            await self._send_to_observer(observer,AE_BUFFER_TIME,((module.mod_id,"out",out_idx),
                                                            buf.get_time()-buf.get_delayed_time()),None)
                        for in_idx in range(module.get_num_inputs()):
                            buf = module.get_in_buf(in_idx)
                            await self._send_to_observer(observer,AE_BUFFER_TIME,((module.mod_id,"in",in_idx),
                                                            buf.get_time()-buf.get_delayed_time()),None)
                    for module in self._sink_modules:
                        for in_idx in range(module.get_num_inputs()):
                            buf = module.get_in_buf(in_idx)
                            await self._send_to_observer(observer,AE_BUFFER_TIME,((module.mod_id,"in",in_idx),
                                                            buf.get_time()-buf.get_delayed_time()),None)
                requires_data = False
                for module in self._sink_modules:
                    requires_data |= module.requires_data
                
        self._running = False
        self._stopping = False
        for module in self._source_modules:
            module.stop()
        for module in self._sink_modules:
            module.stop()

    def open_all(self):
        """Open all modules."""

        for module in self._source_modules+self._internal_modules+self._sink_modules:
            module.open()
        self._opened = True

    def close_all(self):
        """Close all modules."""

        for module in self._source_modules+self._internal_modules+self._sink_modules:
            module.close()
        self._opened = False

    async def start(self, callback, to_gui):
        """Start the audio engine if it is not already started.
        
        Opens all modules prior to starting if the audio engine has not already
        been opened."""

        if not self._opened:
            self.open_all()
        if not self._sources_done and not self._running and not self._stopping:
            self._running = True
            for module in self._source_modules + self._sink_modules:
                module.start()
            callback()
            await self._run(to_gui)

    async def pause(self, callback):
        """Stop (pause) the audio engine."""

        if not self._stopping and self._running:
            self._stopping = True
            while self._stopping:
                await asyncio.sleep(0.1)
            if callback:
                callback()

    async def stop(self, callback):
        """Stop the audio engine and close all modules."""

        if not self._stopping and self._running:
            self._stopping = True
            while self._stopping:
                await asyncio.sleep(0.1)
            self.close_all()
            if callback:
                callback()
        elif not self._running:
            print("AE not running so just closing all.")
            if self._opened:
                self.close_all()
            if callback:
                callback()

    async def reset(self, to_gui):
        """Reset all source modules without stopping processing."""
        for module in self._source_modules:
            module.reset()
    
    async def fastrewind(self, to_gui):
        """Rewind all source modules without stopping processing."""

    async def fastforward(self, rate, to_gui):
        """Increase the processing rate."""

    async def process(self, to_gui):
        """Process at maximum rate."""

    def get_source_modules(self) -> list[am.AudioModule]:
        '''
        Return the list of all source modules.
        '''
        return self._source_modules

    def get_sink_modules(self) -> list[am.AudioModule]:
        '''
        Return the list of all sink modules.
        '''
        return self._sink_modules

    def get_internal_modules(self) -> list[am.AudioModule]:
        '''
        Return the list of all internal modules.
        '''
        return self._internal_modules

    def is_running(self) -> bool:
        '''
        Return true if the audio engine is currently running.
        '''
        return self._running



    
