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

from ast import literal_eval
from collections import deque


class PredecessorTreeNode:
    def __init__(self, children=None, readers=None, writer=None):
        self.children = children if children is not None else {}
        self.readers = readers if readers is not None else []
        self.writer = writer

    def __repr__(self):
        retval = {}

        if len(self.readers) > 0:
            retval['readers'] = self.readers
        if self.writer is not None:
            retval['writer'] = self.writer
        if len(self.children) > 0:
            retval['children'] = \
                {k: literal_eval(repr(v)) for k, v in self.children.items()}

        return repr(retval)


class PredecessorTree:
    def __init__(self, token_size=2):
        self._token_size = token_size
        self._root = PredecessorTreeNode()

    def __repr__(self):
        return repr(self._root)

    def _tokenize_address(self, address):
        return [address[i:i + self._token_size]
                for i in range(0, len(address), self._token_size)]

    def _get(self, address, create=False):
        tokens = self._tokenize_address(address)

        node = self._root
        for token in tokens:
            if token in node.children:
                node = node.children[token]
            else:
                if not create:
                    return None
                child = PredecessorTreeNode()
                node.children[token] = child
                node = child

        return node

    def get(self, address):
        return self._get(address)

    def add_reader(self, address, reader):
        node = self._get(address, create=True)
        node.readers.append(reader)

    def set_writer(self, address, writer):
        node = self._get(address, create=True)
        node.readers = []
        node.writer = writer
        node.children = {}

    def find_write_predecessors(self, address):
        """Returns all predecessor transaction ids for a write of the provided
        address.

        Arguments:
            address (str): the radix address

        Returns: a set of transaction ids
        """
        # A write operation must be preceded by:
        #   - The "enclosing writer", which is the writer at the address or
        #     the nearest writer higher (closer to the root) in the tree.
        #   - The "enclosing readers", which are the readers at the address
        #     or higher in the tree.
        #   - The "children writers", which include all writers which are
        #     lower in the tree than the address.
        #   - The "children readers", which include all readers which are
        #     lower in the tree than the address.
        #
        # The enclosing writer must be added as it may have modified a node
        # which must not happen after the current write.
        #
        # Writers which are higher in the tree than the enclosing writer may
        # have modified a node at or under the given address.  However, we do
        # not need to include them here as they will have been considered a
        # predecessor to the enclosing writer.
        #
        # Enclosing readers must be included.  Technically, we only need to add
        # enclosing readers which occurred after the enclosing writer, since
        # the readers preceding the writer will have been considered a
        # predecessor of the enclosing writer.  However, with the current
        # data structure we can not determine the difference between readers
        # so we specify them all; this is mostly harmless as it will not change
        # the eventual sort order generated by the scheduler.
        #
        # Children readers must be added, since their reads must happen prior
        # to the write.

        predecessors = set()

        tokens = self._tokenize_address(address)

        # First, find enclosing_writer, enclosing_readers, and address_node (if
        # the tree is deep enough).
        enclosing_writer = None
        address_node = None

        node = self._root
        for i, token in enumerate(tokens):
            if token not in node.children:
                break

            node = node.children[token]
            if i == len(tokens) - 1:
                address_node = node

            # add enclosing readers directly to predecessors
            predecessors.update(set(node.readers))

            if node.writer is not None:
                enclosing_writer = node.writer

        if enclosing_writer is not None:
            predecessors.add(enclosing_writer)

        # Next, descend down the tree starting at address_node and find
        # all children writers and readers.  Uses breadth first search.
        to_process = deque()
        to_process.extendleft(address_node.children.values())
        while len(to_process) > 0:
            node = to_process.pop()
            predecessors.update(node.readers)
            if node.writer is not None:
                predecessors.add(node.writer)
            to_process.extendleft(address_node.children.values())

        return predecessors

    def find_read_predecessors(self, address):
        """Returns all predecessor transaction ids for a read of the provided
        address.

        Arguments:
            address (str): the radix address

        Returns: a set of transaction ids
        """
        # A read operation must be preceded by:
        #   - The "enclosing writer", which is the writer at the address or
        #     the nearest writer higher (closer to the root) in the tree.
        #   - All "children writers", which include all writers which are
        #     lower in the tree than the address.
        #
        # The enclosing writer must be added as it is possible it updated the
        # contents stored at address.
        #
        # Writers which are higher in the tree than the enclosing writer may
        # have modified the address.  However, we do not need to include them
        # here as they will have been considered a predecessor to the enclosing
        # writer.
        #
        # Children writers must be included as they may have updated addresses
        # lower in the tree, and these writers will have always been preceded
        # by the enclosing writer.
        #
        # We do not need to add any readers, since a reader cannot impact the
        # value which we are reading.  The relationship is transitive, in that
        # this reader will also not impact the readers already recorded in the
        # tree.

        predecessors = set()

        tokens = self._tokenize_address(address)

        # First, find enclosing_writer and address_node (if the tree is deep
        # enough).
        enclosing_writer = None
        address_node = None

        node = self._root
        for i, token in enumerate(tokens):
            if token not in node.children:
                break

            node = node.children[token]
            if i == len(tokens) - 1:
                address_node = node

            if node.writer is not None:
                enclosing_writer = node.writer

        if enclosing_writer is not None:
            predecessors.add(enclosing_writer)

        # Next, descend down the tree starting at address_node and find
        # all children writers.  Uses breadth first search.
        to_process = deque()
        to_process.extendleft(address_node.children.values())
        while len(to_process) > 0:
            node = to_process.pop()
            if node.writer is not None:
                predecessors.add(node.writer)
            to_process.extendleft(address_node.children.values())

        return predecessors


class TopologicalSorter:
    def __init__(self):
        self._count = {}
        self._successors = {}
        self._identifiers = []

    def _init(self, identifier):
        if identifier not in self._count:
            self._count[identifier] = 0
        if identifier not in self._successors:
            self._successors[identifier] = []
        if identifier not in self._identifiers:
            self._identifiers.append(identifier)

    def add_relation(self, predecessor, successor):
        self._init(predecessor)
        self._init(successor)
        self._count[successor] += 1
        self._successors[predecessor].append(successor)

    def order(self):
        retval = []

        while len(self._identifiers) > 0:
            found = None
            for identifier in self._identifiers:
                if self._count[identifier] == 0:
                    found = identifier
                    break
            if found is not None:
                retval.append(found)
                for successor in self._successors[found]:
                    self._count[successor] -= 1

                self._identifiers.remove(found)
                del self._count[found]
                del self._successors[found]
            else:
                raise Exception("non-acyclic graph detected, aborting")

        return retval
