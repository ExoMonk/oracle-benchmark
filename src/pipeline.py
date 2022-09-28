from src.node import NodeRequester
from src.utils import get_selector_from_name, hex_string_to_decimal
import pandas as pd
import os
import json


class EmpiricLoader:
    """
    Empiric Network DataLoader
    """

    STARKNET_STARTING_BLOCK = 177896
    STARKNET_ENDING_BLOCK = 210236
    EMPIRIC_CONTRACT_ADDRESS = hex_string_to_decimal("0x4a05a68317edb37d34d29f34193829d7363d51a37068f32b142c637e43b47a2")

    def __init__(self):
        self.sequencer_requester = NodeRequester(os.environ.get('STARKNET_SEQUENCER_URL'))
        self.node_requester = NodeRequester(os.environ.get('STARKNET_NODE_URL'))
        self.raw_transactions = []
        self.transactions = pd.DataFrame()
        self.initialize()

    def initialize(self):
        for block_number in range(self.STARKNET_STARTING_BLOCK, self.STARKNET_ENDING_BLOCK):
            params = {
                "block_number": block_number
            }
            r = self.node_requester.post("", method="starknet_getBlockWithTxs", params=[params])
            data = json.loads(r.text)
            if 'error'in data:
                return data['error']
            data = data["result"]
            list_txs = list(filter(
                lambda tx: hex_string_to_decimal(tx['contract_address']) == self.EMPIRIC_CONTRACT_ADDRESS, 
                data['transactions']
            ))
            timestamp = data['timestamp']
            if list_txs:
                new_df = pd.DataFrame(list_txs)
                new_df['timestamp'] = timestamp
                self.transactions = pd.concat([self.transactions, new_df], ignore_index=True)
