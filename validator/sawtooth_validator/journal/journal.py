# Copyright 2016 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

from concurrent.futures import ThreadPoolExecutor
import logging
import queue
from threading import Thread
import time

from sawtooth_validator.journal.publisher import BlockPublisher
from sawtooth_validator.journal.chain import ChainController
from sawtooth_validator.journal.block_cache import BlockCache
from sawtooth_validator.journal.block_store_adapter import BlockStoreAdapter

LOGGER = logging.getLogger(__name__)


class Journal(object):
    """
    Manages the block chain, This responsibility boils down
    1) to evaluating new blocks to determine if they should extend or replace
    the current chain. Handled by the ChainController
    2) Claiming new blocks. Handled by the BlockPublisher.
    This object provides the threading and event queue for the processors.
    """
    class _ChainThread(Thread):
        def __init__(self, chain_controller, block_queue, block_cache):
            Thread.__init__(self)
            self._chain_controller = chain_controller
            self._block_queue = block_queue
            self._block_cache = block_cache
            self._block_cache_purge_time = time.time() + \
                self._block_cache.keep_time
            self._exit = False

        def run(self):
            try:
                while True:
                    try:
                        block = self._block_queue.get(timeout=0.1)
                        self._chain_controller.on_block_received(block)
                    except queue.Empty:
                        time.sleep(0.1)

                    if self._block_cache_purge_time < time.time():
                        self._block_cache.purge_expired()
                        self._block_cache_purge_time = time.time() + \
                            self._block_cache.keep_time

                    if self._exit:
                        return
            # pylint: disable=broad-except
            except Exception as exc:
                LOGGER.exception(exc)
                LOGGER.critical("ChainController thread exited with error.")
            finally:
                LOGGER.debug(
                    "Thread(%s) exited",
                    self.__class__.__name__)

        def stop(self):
            self._exit = True

    class _PublisherThread(Thread):
        def __init__(self, block_publisher, batch_queue):
            Thread.__init__(self)
            self._block_publisher = block_publisher
            self._batch_queue = batch_queue
            self._exit = False

        def run(self):
            try:
                while True:
                    try:
                        batch = self._batch_queue.get(timeout=0.1)
                        self._block_publisher.on_batch_received(batch)
                    except queue.Empty:
                        time.sleep(0.1)

                    self._block_publisher.on_check_publish_block()
                    if self._exit:
                        return
            # pylint: disable=broad-except
            except Exception as exc:
                LOGGER.exception(exc)
                LOGGER.critical("BlockPublisher thread exited with error.")
            finally:
                LOGGER.debug(
                    "Thread(%s) exited",
                    self.__class__.__name__)

        def stop(self):
            self._exit = True

    def __init__(self,
                 consensus_module,
                 block_store,
                 state_view_factory,
                 block_sender,
                 transaction_executor,
                 squash_handler,
                 block_cache=None  # not require, allows tests to inject a
                 # prepopulated block cache.
                 ):
        self._consensus_module = consensus_module
        self._block_store = BlockStoreAdapter(block_store)
        self._block_cache = block_cache
        if self._block_cache is None:
            self._block_cache = BlockCache(self._block_store)
        self._state_view_factory = state_view_factory

        self._transaction_executor = transaction_executor
        self._squash_handler = squash_handler
        self._block_sender = block_sender

        self._block_publisher = None
        self._batch_queue = queue.Queue()
        self._publisher_thread = None

        self._chain_controller = None
        self._block_queue = queue.Queue()
        self._chain_thread = None

    def _init_subprocesses(self):
        self._block_publisher = BlockPublisher(
            consensus_module=self._consensus_module,
            transaction_executor=self._transaction_executor,
            block_cache=self._block_cache,
            state_view_factory=self._state_view_factory,
            block_sender=self._block_sender,
            squash_handler=self._squash_handler,
            chain_head=self._block_store.chain_head
        )
        self._publisher_thread = self._PublisherThread(self._block_publisher,
                                                       self._batch_queue)
        self._chain_controller = ChainController(
            consensus_module=self._consensus_module,
            block_sender=self._block_sender,
            block_cache=self._block_cache,
            state_view_factory=self._state_view_factory,
            executor=ThreadPoolExecutor(1),
            transaction_executor=self._transaction_executor,
            on_chain_updated=self._block_publisher.on_chain_updated,
            squash_handler=self._squash_handler
        )
        self._chain_thread = self._ChainThread(self._chain_controller,
                                               self._block_queue,
                                               self._block_cache)

    # FXM: this is an inaccurate name.
    def get_current_root(self):
        return self._chain_controller.chain_head.state_root_hash

    def get_block_store(self):
        return self._block_store

    def start(self):
        if self._publisher_thread is None and self._chain_thread is None:
            self._init_subprocesses()

        self._publisher_thread.start()
        self._chain_thread.start()

    def stop(self):
        # time to murder the child threads. First ask politely for
        # suicide
        if self._publisher_thread is not None:
            self._publisher_thread.stop()
            self._publisher_thread = None

        if self._chain_thread is not None:
            self._chain_thread.stop()
            self._chain_thread = None

    def on_block_received(self, block):
        """
        New block has been received, queue it with the chain controller
        for processing.
        """
        self._block_queue.put(block)

    def on_batch_received(self, batch):
        """
        New batch has been received, queue it with the BlockPublisher for
        inclusion in the next block.
        """
        self._batch_queue.put(batch)
