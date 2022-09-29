from src.node import NodeRequester
from src.utils import get_selector_from_name, hex_string_to_decimal
from ctc.protocols import chainlink_utils
from ctc.config import get_data_dir
from os.path import exists
import pandas as pd
import os
import json


class EmpiricNetworkLoader:
    """
    Empiric Network DataLoader
    """

    STARKNET_STARTING_BLOCK = 177896
    STARKNET_ENDING_BLOCK = 210236
    EMPIRIC_CONTRACT_ADDRESS = hex_string_to_decimal("0x4a05a68317edb37d34d29f34193829d7363d51a37068f32b142c637e43b47a2")
    EMPIRIC_DATA_FILE = 'data/empiric_txs.csv'

    def __init__(self):
        self.sequencer_requester = NodeRequester(os.environ.get('STARKNET_SEQUENCER_URL'))
        self.node_requester = NodeRequester(os.environ.get('STARKNET_NODE_URL'))
        self.raw_transactions = pd.DataFrame()
        file_exists = exists(self.EMPIRIC_DATA_FILE)
        if file_exists:
            self._load()
        else:
            self._initialize()

    def _initialize(self):
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
                self.raw_transactions = pd.concat([self.transactions, new_df], ignore_index=True)
        self.raw_transactions.to_csv(self.EMPIRIC_DATA_FILE)

    def _load(self):
        self.raw_transactions = pd.read_csv(self.EMPIRIC_DATA_FILE, index_col=0)


class ChainLinkLoader:
    """
    ChainLink DataLoader
    """

    ETH_STARTING_BLOCK = 14720259
    ETH_ENDING_BLOCK = 14850893

    CHAINLINK_LUNA_FEED = "0x91e9331556ed76c9393055719986409e11b56f73"
    CHAINLINK_DATA_DIR = f'{get_data_dir()}/evm/networks/mainnet/events'

    async def __new__(cls, *a, **kw):
        instance = super().__new__(cls)
        await instance.__init__(*a, **kw)
        return instance

    async def __init__(self):
        self.price_feed = pd.DataFrame()
        self.raw_transactions = pd.DataFrame()
        self._load()
        if self.raw_transactions.empty:
            await self._initialize()
            self._load()
        self._format()

    async def _initialize(self):
        try:
            data = await chainlink_utils.async_get_feed_data(self.CHAINLINK_LUNA_FEED, start_block=self.ETH_STARTING_BLOCK, end_block=self.ETH_ENDING_BLOCK)
        except:
            pass

    def _load(self):
        try:
            for root, dirs, files in os.walk(self.CHAINLINK_DATA_DIR):
                files = list(filter(lambda filename: '.csv' in filename, files))
                for name in files:
                    fpath = os.path.join(root, name)
                    new_data = pd.read_csv(fpath)
                    self.raw_transactions = pd.concat([self.raw_transactions, new_data], ignore_index=True)
            self.raw_transactions.sort_values(by=['block_number'], inplace=True)
            self.raw_transactions.reset_index(drop=True, inplace=True)
        except:
            pass

    def _format(self):
        self.transactions['price'] = self.raw_transactions['arg__current'] / 10 ** 18
        self.transactions['timestamp'] = self.raw_transactions['arg__updatedAt']
