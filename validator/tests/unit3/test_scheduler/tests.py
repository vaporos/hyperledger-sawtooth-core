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

import hashlib
import unittest
import bitcoin
import time

import sawtooth_validator.protobuf.batch_pb2 as batch_pb2
import sawtooth_validator.protobuf.transaction_pb2 as transaction_pb2

from sawtooth_validator.execution.context_manager import ContextManager
from sawtooth_validator.execution.scheduler_parallel import PredecessorTree
from sawtooth_validator.execution.scheduler_exceptions import SchedulerError
from sawtooth_validator.execution.scheduler_serial import SerialScheduler
from sawtooth_validator.execution.scheduler_parallel import ParallelScheduler
from sawtooth_validator.database import dict_database


def create_transaction(name, private_key, public_key):
    payload = name
    addr = '000000' + hashlib.sha512(name.encode()).hexdigest()

    header = transaction_pb2.TransactionHeader(
        signer_pubkey=public_key,
        family_name='scheduler_test',
        family_version='1.0',
        inputs=[addr],
        outputs=[addr],
        dependencies=[],
        payload_encoding="application/cbor",
        payload_sha512=hashlib.sha512(payload.encode()).hexdigest(),
        batcher_pubkey=public_key)

    header_bytes = header.SerializeToString()

    signature = bitcoin.ecdsa_sign(
        header_bytes,
        private_key)

    transaction = transaction_pb2.Transaction(
        header=header_bytes,
        payload=payload.encode(),
        header_signature=signature)

    return transaction


def create_batch(transactions, private_key, public_key):
    transaction_ids = [t.header_signature for t in transactions]

    header = batch_pb2.BatchHeader(
        signer_pubkey=public_key,
        transaction_ids=transaction_ids)

    header_bytes = header.SerializeToString()

    signature = bitcoin.ecdsa_sign(
        header_bytes,
        private_key)

    batch = batch_pb2.Batch(
        header=header_bytes,
        transactions=transactions,
        header_signature=signature)

    return batch


class TestSerialScheduler(unittest.TestCase):
    def test_transaction_order(self):
        """Tests the that transactions are returned in order added.

        Adds three batches with varying number of transactions, then tests
        that they are returned in the appropriate order when using an iterator.

        This test also creates a second iterator and verifies that both
        iterators return the same transactions.

        This test also finalizes the scheduler and verifies that StopIteration
        is thrown by the iterator.
        """
        private_key = bitcoin.random_key()
        public_key = bitcoin.encode_pubkey(
            bitcoin.privkey_to_pubkey(private_key), "hex")
        context_manager = ContextManager(dict_database.DictDatabase())
        squash_handler = context_manager.get_squash_handler()
        first_state_root = context_manager.get_first_root()
        scheduler = SerialScheduler(squash_handler, first_state_root)

        txns = []

        for names in [['a', 'b', 'c'], ['d', 'e'], ['f', 'g', 'h', 'i']]:
            batch_txns = []
            for name in names:
                txn = create_transaction(
                    name=name,
                    private_key=private_key,
                    public_key=public_key)

                batch_txns.append(txn)
                txns.append(txn)

            batch = create_batch(
                transactions=batch_txns,
                private_key=private_key,
                public_key=public_key)

            scheduler.add_batch(batch)

        scheduler.finalize()

        iterable1 = iter(scheduler)
        iterable2 = iter(scheduler)
        for txn in txns:
            scheduled_txn_info = next(iterable1)
            self.assertEqual(scheduled_txn_info, next(iterable2))
            self.assertIsNotNone(scheduled_txn_info)
            self.assertEquals(txn.payload, scheduled_txn_info.txn.payload)
            scheduler.set_transaction_execution_result(
                txn.header_signature, False, None)

        with self.assertRaises(StopIteration):
            next(iterable1)

    def test_set_status(self):
        """Tests that set_status() has the correct behavior.

        Basically:
            1. Adds a batch which has two transactions.
            2. Calls next_transaction() to get the first Transaction.
            3. Calls next_transaction() to verify that it returns None.
            4. Calls set_status() to mark the first transaction applied.
            5. Calls next_transaction() to  get the second Transaction.

        Step 3 returns None because the first transaction hasn't been marked
        as applied, and the SerialScheduler will only return one
        not-applied Transaction at a time.

        Step 5 is expected to return the second Transaction, not None,
        since the first Transaction was marked as applied in the previous
        step.
        """
        private_key = bitcoin.random_key()
        public_key = bitcoin.encode_pubkey(
            bitcoin.privkey_to_pubkey(private_key), "hex")

        context_manager = ContextManager(dict_database.DictDatabase())
        squash_handler = context_manager.get_squash_handler()
        first_state_root = context_manager.get_first_root()
        scheduler = SerialScheduler(squash_handler, first_state_root)

        txns = []

        for name in ['a', 'b']:
            txn = create_transaction(
                name=name,
                private_key=private_key,
                public_key=public_key)

            txns.append(txn)

        batch = create_batch(
            transactions=txns,
            private_key=private_key,
            public_key=public_key)

        scheduler.add_batch(batch)

        scheduled_txn_info = scheduler.next_transaction()
        self.assertIsNotNone(scheduled_txn_info)
        self.assertEquals('a', scheduled_txn_info.txn.payload.decode())

        self.assertIsNone(scheduler.next_transaction())

        scheduler.set_transaction_execution_result(
            scheduled_txn_info.txn.header_signature,
            is_valid=False,
            context_id=None)

        scheduled_txn_info = scheduler.next_transaction()
        self.assertIsNotNone(scheduled_txn_info)
        self.assertEquals('b', scheduled_txn_info.txn.payload.decode())

    def test_valid_batch_invalid_batch(self):
        """Tests the squash function. That the correct hash is being used
        for each txn and that the batch ending state hash is being set.

         Basically:
            1. Adds two batches, one where all the txns are valid,
               and one where one of the txns is invalid.
            2. Run through the scheduler executor interaction
               as txns are processed.
            3. Verify that the valid state root is obtained
               through the squash function.
            4. Verify that correct batch statuses are set
        """
        private_key = bitcoin.random_key()
        public_key = bitcoin.encode_pubkey(
            bitcoin.privkey_to_pubkey(private_key), "hex")

        context_manager = ContextManager(dict_database.DictDatabase())
        squash_handler = context_manager.get_squash_handler()
        first_state_root = context_manager.get_first_root()
        scheduler = SerialScheduler(squash_handler, first_state_root)
        # 1)
        batch_signatures = []
        for names in [['a', 'b'], ['invalid', 'c']]:
            batch_txns = []
            for name in names:
                txn = create_transaction(
                    name=name,
                    private_key=private_key,
                    public_key=public_key)

                batch_txns.append(txn)

            batch = create_batch(
                transactions=batch_txns,
                private_key=private_key,
                public_key=public_key)

            batch_signatures.append(batch.header_signature)
            scheduler.add_batch(batch)
        scheduler.finalize()
        # 2)
        sched1 = iter(scheduler)
        invalid_payload = hashlib.sha512('invalid'.encode()).hexdigest()
        while not scheduler.complete(block=False):
            txn_info = next(sched1)
            txn_header = transaction_pb2.TransactionHeader()
            txn_header.ParseFromString(txn_info.txn.header)
            inputs_or_outputs = list(txn_header.inputs)
            c_id = context_manager.create_context(txn_info.state_hash,
                                                  inputs_or_outputs,
                                                  inputs_or_outputs)
            if txn_header.payload_sha512 == invalid_payload:
                scheduler.set_transaction_execution_result(
                    txn_info.txn.header_signature, False, c_id)
            else:
                context_manager.set(c_id, [{inputs_or_outputs[0]: 1}])
                scheduler.set_transaction_execution_result(
                    txn_info.txn.header_signature, True, c_id)

        sched2 = iter(scheduler)
        # 3)
        txn_info_a = next(sched2)
        self.assertEquals(first_state_root, txn_info_a.state_hash)

        txn_a_header = transaction_pb2.TransactionHeader()
        txn_a_header.ParseFromString(txn_info_a.txn.header)
        inputs_or_outputs = list(txn_a_header.inputs)
        address_a = inputs_or_outputs[0]
        c_id_a = context_manager.create_context(first_state_root,
                                                inputs_or_outputs,
                                                inputs_or_outputs)
        context_manager.set(c_id_a, [{address_a: 1}])
        state_root2 = context_manager.commit_context([c_id_a], virtual=False)
        txn_info_b = next(sched2)

        self.assertEquals(txn_info_b.state_hash, state_root2)

        txn_b_header = transaction_pb2.TransactionHeader()
        txn_b_header.ParseFromString(txn_info_b.txn.header)
        inputs_or_outputs = list(txn_b_header.inputs)
        address_b = inputs_or_outputs[0]
        c_id_b = context_manager.create_context(state_root2,
                                                inputs_or_outputs,
                                                inputs_or_outputs)
        context_manager.set(c_id_b, [{address_b: 1}])
        state_root3 = context_manager.commit_context([c_id_b], virtual=False)
        txn_infoInvalid = next(sched2)

        self.assertEquals(txn_infoInvalid.state_hash, state_root3)

        txn_info_c = next(sched2)
        self.assertEquals(txn_info_c.state_hash, state_root3)
        # 4)
        batch1_result = scheduler.get_batch_execution_result(
            batch_signatures[0])
        self.assertTrue(batch1_result.is_valid)
        self.assertEquals(batch1_result.state_hash, state_root3)

        batch2_result = scheduler.get_batch_execution_result(
            batch_signatures[1])
        self.assertFalse(batch2_result.is_valid)
        self.assertIsNone(batch2_result.state_hash)


class TestPredecessorTree(unittest.TestCase):

    def test_predecessor_tree(self):
        """Tests basic functionality of the scheduler's radix tree.
        """
        address_a = \
            'ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb'
        address_b = \
            '3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d'

        tree = PredecessorTree()
        tree.add_reader(address_a, 'txn1')
        tree.add_reader(address_b, 'txn2')

        node = tree.get(address_a)
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, ['txn1'])
        self.assertIsNone(node.writer)
        self.assertEqual(node.children, {})

        node = tree.get(address_b)
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, ['txn2'])
        self.assertIsNone(node.writer, None)
        self.assertEqual(node.children, {})

        # Set a writer for address_a.
        tree.set_writer(address_a, 'txn1')

        # Verify address_a now contains txn1 as the writer, with no
        # readers set.
        node = tree.get(address_a)
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, [])
        self.assertEqual(node.writer, 'txn1')
        self.assertEqual(node.children, {})

        # Verify address_b didn't change when address_a was modified.
        node = tree.get(address_b)
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, ['txn2'])
        self.assertIsNone(node.writer)
        self.assertEqual(node.children, {})

        # Set a writer for a prefix of address_b.
        tree.set_writer(address_b[0:4], 'txn3')

        # Verify address_b[0:4] now contains txn3 as the writer, with
        # no readers set and no children.
        node = tree.get(address_b[0:4])
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, [])
        self.assertEqual(node.writer, 'txn3')
        self.assertEqual(node.children, {})

        # Verify address_b now returns None
        node = tree.get(address_b)
        self.assertIsNone(node)

        # Verify address_a didn't change when address_b[0:4] was modified.
        node = tree.get(address_a)
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, [])
        self.assertEqual(node.writer, 'txn1')
        self.assertEqual(node.children, {})

        # Add readers for address_a, address_b
        tree.add_reader(address_a, 'txn1')
        tree.add_reader(address_b, 'txn2')

        node = tree.get(address_a)
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, ['txn1'])
        self.assertEqual(node.writer, 'txn1')
        self.assertEqual(node.children, {})

        node = tree.get(address_b)
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, ['txn2'])
        self.assertIsNone(node.writer)
        self.assertEqual(node.children, {})

        # Verify address_b[0:4] now contains txn3 as the writer, with
        # no readers set and 'e8' as a child.
        node = tree.get(address_b[0:4])
        self.assertIsNotNone(node)
        self.assertEqual(node.readers, [])
        self.assertEqual(node.writer, 'txn3')
        self.assertEqual(list(node.children.keys()), ['e8'])

        self.assertEqual(
            tree.find_write_predecessors(address_a),
            set(['txn1']))
        self.assertEqual(
            tree.find_read_predecessors(address_a),
            set(['txn1']))
        self.assertEqual(
            tree.find_write_predecessors(address_b),
            set(['txn3', 'txn2']))
        self.assertEqual(
            tree.find_read_predecessors(address_b),
            set(['txn3']))


class TestParallelScheduler(unittest.TestCase):
    @staticmethod
    def _create_transaction(name, private_key, public_key, inputs=None,
                            outputs=None):
        payload = name
        addr = '000000' + hashlib.sha512(name.encode()).hexdigest()

        if inputs is None:
            inputs = [addr]
        else:
            inputs = inputs.copy().append(addr)

        if outputs is None:
            outputs = [addr]
        else:
            outputs.copy().append(addr)

        header = transaction_pb2.TransactionHeader(
            signer_pubkey=public_key,
            family_name='scheduler_test',
            family_version='1.0',
            inputs=inputs,
            outputs=outputs,
            dependencies=[],
            nonce=str(time.time()),
            payload_encoding="application/cbor",
            payload_sha512=hashlib.sha512(payload.encode()).hexdigest(),
            batcher_pubkey=public_key)

        header_bytes = header.SerializeToString()

        signature = bitcoin.ecdsa_sign(
            header_bytes,
            private_key)

        transaction = transaction_pb2.Transaction(
            header=header_bytes,
            payload=payload.encode(),
            header_signature=signature)

        return transaction, header

    @staticmethod
    def _create_batch(transactions, private_key, public_key):
        transaction_ids = [t.header_signature for t in transactions]

        header = batch_pb2.BatchHeader(
            signer_pubkey=public_key,
            transaction_ids=transaction_ids)

        header_bytes = header.SerializeToString()

        signature = bitcoin.ecdsa_sign(
            header_bytes,
            private_key)

        batch = batch_pb2.Batch(
            header=header_bytes,
            transactions=transactions,
            header_signature=signature)

        return batch

    def test_add_to_finalized_scheduler(self):
        """Tests that a finalized scheduler raise exception on add_batch().

        This test creates a scheduler, finalizes it, and calls add_batch().
        The result is expected to be a SchedulerError, since adding a batch
        to a finalized scheduler is invalid.
        """
        private_key = bitcoin.random_key()
        public_key = bitcoin.encode_pubkey(
            bitcoin.privkey_to_pubkey(private_key), "hex")
        context_manager = ContextManager(dict_database.DictDatabase())
        squash_handler = context_manager.get_squash_handler()
        first_state_root = context_manager.get_first_root()
        scheduler = ParallelScheduler(squash_handler, first_state_root)

        # Finalize prior to attempting to add a batch.
        scheduler.finalize()

        txn, _ = TestParallelScheduler._create_transaction(
            name='a',
            private_key=private_key,
            public_key=public_key)

        batch = TestParallelScheduler._create_batch(
            transactions=[txn],
            private_key=private_key,
            public_key=public_key)

        # scheduler.add_batch(batch) should throw a SchedulerError due to
        # the finalized status of the scheduler.
        self.assertRaises(SchedulerError, lambda: scheduler.add_batch(batch))

    def test_set_result_on_unscheduled_txn(self):
        """Tests that a scheduler will reject a result on an unscheduled
        transaction.

        Creates a batch with a single transaction, adds the batch to the
        scheduler, then immediately attempts to set the result for the
        transaction without first causing it to be scheduled (by using an
        iterator or calling next_transaction()).
        """
        private_key = bitcoin.random_key()
        public_key = bitcoin.encode_pubkey(
            bitcoin.privkey_to_pubkey(private_key), "hex")
        context_manager = ContextManager(dict_database.DictDatabase())
        squash_handler = context_manager.get_squash_handler()
        first_state_root = context_manager.get_first_root()
        scheduler = ParallelScheduler(squash_handler, first_state_root)

        txn, _ = TestParallelScheduler._create_transaction(
            name='a',
            private_key=private_key,
            public_key=public_key)

        batch = TestParallelScheduler._create_batch(
            transactions=[txn],
            private_key=private_key,
            public_key=public_key)

        scheduler.add_batch(batch)

        self.assertRaises(
            SchedulerError,
            lambda: scheduler.set_transaction_execution_result(
                txn.header_signature, False, None))


    def test_transaction_order(self):
        """Tests the that transactions are returned in order added.

        Adds three batches with varying number of transactions, then tests
        that they are returned in the appropriate order when using an iterator.

        This test also creates a second iterator and verifies that both
        iterators return the same transactions.

        This test also finalizes the scheduler and verifies that StopIteration
        is thrown by the iterator.
        """
        private_key = bitcoin.random_key()
        public_key = bitcoin.encode_pubkey(
            bitcoin.privkey_to_pubkey(private_key), "hex")
        context_manager = ContextManager(dict_database.DictDatabase())
        squash_handler = context_manager.get_squash_handler()
        first_state_root = context_manager.get_first_root()
        scheduler = ParallelScheduler(squash_handler, first_state_root)

        txns = []

        for names in [['a', 'b', 'c'], ['d', 'e'], ['f', 'g', 'h', 'i']]:
            batch_txns = []
            for name in names:
                txn, _ = TestParallelScheduler._create_transaction(
                    name=name,
                    private_key=private_key,
                    public_key=public_key)

                batch_txns.append(txn)
                txns.append(txn)

            batch = TestParallelScheduler._create_batch(
                transactions=batch_txns,
                private_key=private_key,
                public_key=public_key)

            scheduler.add_batch(batch)

        iterable1 = iter(scheduler)
        iterable2 = iter(scheduler)
        for txn in txns:
            scheduled_txn_info = next(iterable1)
            self.assertEqual(scheduled_txn_info, next(iterable2))
            self.assertIsNotNone(scheduled_txn_info)
            self.assertEquals(txn.payload, scheduled_txn_info.txn.payload)
            scheduler.set_transaction_execution_result(
                txn.header_signature, False, None)

        scheduler.finalize()
        self.assertTrue(scheduler.complete(block=False))
        with self.assertRaises(StopIteration):
            next(iterable1)

    def test_transaction_order_with_dependencies(self):
        """Tests the that transactions are returned in the expected order given
        dependencies implied by state.

        Creates one batch with four transactions.
        """
        private_key = bitcoin.random_key()
        public_key = bitcoin.encode_pubkey(
            bitcoin.privkey_to_pubkey(private_key), "hex")
        context_manager = ContextManager(dict_database.DictDatabase())
        squash_handler = context_manager.get_squash_handler()
        first_state_root = context_manager.get_first_root()
        scheduler = ParallelScheduler(squash_handler, first_state_root)

        txns = []
        headers = []

        txn, header = TestParallelScheduler._create_transaction(
            name='a',
            private_key=private_key,
            public_key=public_key)
        txns.append(txn)
        headers.append(header)

        txn, header = TestParallelScheduler._create_transaction(
            name='b',
            private_key=private_key,
            public_key=public_key)
        txns.append(txn)
        headers.append(header)

        txn, header = TestParallelScheduler._create_transaction(
            name='aa',
            private_key=private_key,
            public_key=public_key,
            inputs=['000000' + hashlib.sha512('a'.encode()).hexdigest()],
            outputs=['000000' + hashlib.sha512('a'.encode()).hexdigest()])
        txns.append(txn)
        headers.append(header)

        txn, header = TestParallelScheduler._create_transaction(
            name='bb',
            private_key=private_key,
            public_key=public_key,
            inputs=['000000' + hashlib.sha512('b'.encode()).hexdigest()],
            outputs=['000000' + hashlib.sha512('b'.encode()).hexdigest()])
        txns.append(txn)
        headers.append(header)

        batch = TestParallelScheduler._create_batch(
            transactions=txns,
            private_key=private_key,
            public_key=public_key)

        scheduler.add_batch(batch)
        scheduler.finalize()
        self.assertFalse(scheduler.complete(block=False))

        iterable = iter(scheduler)
        scheduled_txn_info = []

        self.assertEquals(2, scheduler.available())
        scheduled_txn_info.append(next(iterable))
        self.assertIsNotNone(scheduled_txn_info[0])
        self.assertEquals(txns[0].payload, scheduled_txn_info[0].txn.payload)
        self.assertFalse(scheduler.complete(block=False))

        self.assertEquals(1, scheduler.available())
        scheduled_txn_info.append(next(iterable))
        self.assertIsNotNone(scheduled_txn_info[1])
        self.assertEquals(txns[1].payload, scheduled_txn_info[1].txn.payload)
        self.assertFalse(scheduler.complete(block=False))

        self.assertEquals(0, scheduler.available())
        context_id = context_manager.create_context(
            state_hash=first_state_root,
            inputs=list(headers[1].inputs),
            outputs=list(headers[1].outputs))
        scheduler.set_transaction_execution_result(
            txns[1].header_signature, True, context_id)

        self.assertEquals(1, scheduler.available())
        scheduled_txn_info.append(next(iterable))
        self.assertIsNotNone(scheduled_txn_info[2])
        self.assertEquals(txns[3].payload, scheduled_txn_info[2].txn.payload)
        self.assertFalse(scheduler.complete(block=False))

        self.assertEquals(0, scheduler.available())
        context_id = context_manager.create_context(
            state_hash=first_state_root,
            inputs=list(headers[0].inputs),
            outputs=list(headers[0].outputs))
        scheduler.set_transaction_execution_result(
            txns[0].header_signature, True, context_id)

        self.assertEquals(1, scheduler.available())
        scheduled_txn_info.append(next(iterable))
        self.assertIsNotNone(scheduled_txn_info[3])
        self.assertEquals(txns[2].payload, scheduled_txn_info[3].txn.payload)
        self.assertTrue(scheduler.complete(block=False))

        self.assertEquals(0, scheduler.available())
        context_id = context_manager.create_context(
            state_hash=first_state_root,
            inputs=list(headers[2].inputs),
            outputs=list(headers[2].outputs))
        scheduler.set_transaction_execution_result(
            txns[2].header_signature, True, context_id)
        context_id = context_manager.create_context(
            state_hash=first_state_root,
            inputs=list(headers[3].inputs),
            outputs=list(headers[3].outputs))
        scheduler.set_transaction_execution_result(
            txns[3].header_signature, True, context_id)

        self.assertEquals(0, scheduler.available())
        self.assertTrue(scheduler.complete(block=False))
        with self.assertRaises(StopIteration):
            next(iterable)

        result = scheduler.get_batch_execution_result(batch.header_signature)
        self.assertIsNotNone(result)
        self.assertTrue(result.is_valid)
