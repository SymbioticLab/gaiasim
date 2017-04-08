import logging

# setup logger
logger = logging.getLogger(__name__)


class Link(object):
    def __init__(self, bandwidth, start, end):
        """
        :param bandwidth: the given bandwidth of the network
        :param start: start_node_id
        :param end: end_node_id
        """
        self.total_bandwidth = round(float(bandwidth), 2)  # total_bandwidth is all the bandwidth this link has
        self.allocated_bandwidth = round(float(0), 2)  # allocated_bandwidth is the bandwidth that already be used
        self.bandwidth = round(float(bandwidth), 2)  # bandwidth is the remaining bandwidth of this link
        self.start_node_id = start
        self.end_node_id = end
        self.flow_dict = dict()  # used for max-min scheduler

    def allocate(self, bandwidth):
        bandwidth = round(bandwidth, 2)
        bandwidth_shortage = round(self.bandwidth, 2) - bandwidth
        # print 'self.bandwidth: %f, try to allocate bandwidth: %f' % (self.bandwidth, bandwidth)
        # logger.debug('Bandwidth allocation on link ' + self.__str__() + '\n'
        #              + 'self bandwidth: %f, try to allocate bandwidth: %f' % (self.bandwidth, bandwidth))
        if round(bandwidth_shortage, 2) < round(-0.01, 2):
            assert round(bandwidth_shortage, 2) >= round(-0.01, 2), 'self.bandwidth: %f, try to allocate bandwidth: %f' % (
                self.bandwidth, bandwidth
            )
        self.allocated_bandwidth += bandwidth
        self.bandwidth -= bandwidth
        if round(self.bandwidth, 2) == round(0.01, 2):
            # precision problem: 1.0 - 0.99 == 0.009999999999998 != 0.01
            self.bandwidth = round(0.01, 2)
        elif abs(round(self.bandwidth, 2)) < round(0.01, 2):
            self.bandwidth = round(0, 2)
            self.allocated_bandwidth = round(self.total_bandwidth, 2)
        self.__check_bandwidth()

    def release(self, bandwidth=None):
        bandwidth = round(bandwidth or self.allocated_bandwidth, 2)
        self.bandwidth += bandwidth
        self.allocated_bandwidth -= bandwidth
        if round(self.allocated_bandwidth, 2) <= 0.01:
            self.bandwidth = round(self.total_bandwidth, 2)
            self.allocated_bandwidth = round(0, 2)
        self.__check_bandwidth()

    def check_bandwidth_assign(self, bandwidth_to_check):
        if round(bandwidth_to_check, 2) > round(self.bandwidth, 2):
            return round(self.bandwidth, 2)
        else:
            return round(bandwidth_to_check, 2)

    def __check_bandwidth(self):
        assert round(self.allocated_bandwidth, 2) + round(self.bandwidth, 2) == round(self.total_bandwidth, 2)

    @property
    def ids(self):
        return self.start_node_id, self.end_node_id

    def swap_ids(self):
        self.start_node_id, self.end_node_id = self.end_node_id, self.start_node_id

    def __str__(self):
        return "(" + str(self.start_node_id) + "," + str(self.end_node_id) \
               + ": " + str(self.bandwidth) + "/" + str(self.total_bandwidth) + ")"


class LinkDict(object):
    def __init__(self):
        self.dict = dict()

    def add(self, link):
        if not self.is_duplicate(link):
            self.dict[link.ids] = link

    def get(self, start_node_id, end_node_id):
        ids = (start_node_id, end_node_id)
        if ids in self.dict:
            return self.dict[ids]
        elif ids[::-1] in self.dict:
            return self.dict[ids[::-1]]
        else:
            return None

    def get_valid_neighbors(self, nid):
        neighbors = []
        for ids in self.dict:
            if self.dict[ids].bandwidth == 0:
                continue
            ids_set = set(ids)
            if nid in ids_set:
                new_ids = ids_set.difference({nid})
                assert len(new_ids) == 1
                neighbors.extend(new_ids)
        return neighbors

    def is_duplicate(self, link):
        if link.ids in self.dict or link.ids[::-1] in self.dict:
            return True
        return False

    def get_link_kv(self):
        link_kv = dict()
        for k, link in self.dict.items():
            link_kv[k] = link.bandwidth
        return link_kv

    def get_dict(self):
        return self.dict

    def __len__(self):
        return len(self.dict)


class BidirectionalLinkDict(LinkDict):
    def __init__(self):
        super(BidirectionalLinkDict, self).__init__()

    def get(self, start_node_id, end_node_id):
        ids = (start_node_id, end_node_id)
        if ids in self.dict:
            return self.dict[ids]
        else:
            return None

    def is_duplicate(self, link):
        if link.ids in self.dict:
            return True
        return False
