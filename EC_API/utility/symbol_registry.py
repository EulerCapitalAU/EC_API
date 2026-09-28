#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 16:28:45 2026

@author: dexter
"""

from EC_API._typing import ContractMetaDataType
from EC_API.exceptions import (
    FailRegisterError,
    SymbolNotInRegistryError,
    MetaDataMissingError,
    DuplicateSymbolError,
)


class SymbolRegistry:
    """
    Note that the operations for metadata and symbol-contract_id pairs are
    distinct. To make sure both are in the registry, use the register() and
    deregister() functions.
    """

    def __init__(self):
        self._sym_to_contract_ids: dict[str, int] = dict()
        self._metadata: dict[str, ContractMetaDataType] = dict()
        self._contract_id_to_sym: dict[int, str] = dict()


    # --- property
    @property
    def active_symbols(self):
        return list(self._sym_to_contract_ids.keys())

    @property
    def metatdata(self):
        return self._metadata

    @property
    def sym_to_contract_ids(self):
        return self._sym_to_contract_ids

    # --- CRUD functions
    # ---Symbols and contract_id
    def add_symbol(self, symbol_name: str, contract_id: int) -> bool:
        if symbol_name in self._sym_to_contract_ids:
            raise DuplicateSymbolError(
                f"MetaData of Symbol: {symbol_name} is already in the registry."
            )
        self._sym_to_contract_ids[symbol_name] = contract_id
        self._contract_id_to_sym[contract_id] = symbol_name
        return True

    def remove_symbol(
        self,
        symbol_name: str,
    ) -> bool:
        if symbol_name not in self._sym_to_contract_ids:
            raise SymbolNotInRegistryError(f"Symbol: {symbol_name} is not in the registry.")
            
        self._contract_id_to_sym.pop(self._sym_to_contract_ids[symbol_name])
        self._sym_to_contract_ids.pop(symbol_name)
        return True

    # --- Metadata
    def add_metadata(self, symbol_name: str, metadata: ContractMetaDataType) -> bool:
        if symbol_name in self._metadata.keys():
            raise DuplicateSymbolError(
                f"MetaData of Symbol: {symbol_name} is already in the registry."
            )

        self._metadata[symbol_name] = metadata
        return True

    def remove_metadata(self, symbol_name: str) -> bool:
        if symbol_name not in self._metadata.keys():
            raise MetaDataMissingError(f"MetaData of Symbol: {symbol_name} is not in the registry.")
        self._metadata.pop(symbol_name)
        return True

    # --- register + deregister function
    def register(self, symbol_name: str, metadata: ContractMetaDataType) -> bool:
        self.add_symbol(symbol_name, metadata["contract_id"])
        try:
            self.add_metadata(symbol_name, metadata)
            return True

        except DuplicateSymbolError as e:
            self.remove_symbol(symbol_name)
            raise FailRegisterError(f"Failed to register symbol: {symbol_name}.") from e

    def deregister(self, symbol_name: str) -> bool:
        self.remove_symbol(symbol_name)
        self.remove_metadata(symbol_name)

        return True

    # --- getter methods
    def get_contract_ids(self, symbol_name: str) -> int:
        if self._sym_to_contract_ids.get(symbol_name) is None:
            raise SymbolNotInRegistryError(
                f"Symbol: {symbol_name} is not in the registry.",
            )
        return self._sym_to_contract_ids.get(symbol_name)

    def get_metadata(self, symbol_name: str) -> ContractMetaDataType:
        if not self._metadata.get(symbol_name):
            raise MetaDataMissingError(
                f"MetaData for symbol: {symbol_name} is missing",
            )
        return self._metadata.get(symbol_name)
    
    def get_symbol_name(self, contract_id: int) -> str:
        if self._contract_id_to_sym.get(contract_id) is None:
            raise SymbolNotInRegistryError(
                f"Contract_id: {contract_id} is not in the registry.",
            )
        return self._contract_id_to_sym.get(contract_id)

    # --- inqury functions
    def has_symbol(self, symbol_name: str) -> bool:
        if symbol_name in self._sym_to_contract_ids.keys() and symbol_name in self._metadata.keys():
            return True
        else:
            return False
