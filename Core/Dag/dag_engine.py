import networkx as nx
import threading
import json
import os
from hashlib import sha256
from datetime import datetime

class Transaction:
    def _init_(self, tx_id, parent_ids=None, data=None, signature=None, timestamp=None):
        self.tx_id = tx_id
        self.parent_ids = parent_ids or []
        self.data = data or {}
        self.signature = signature
        self.timestamp = timestamp or datetime.utcnow().isoformat()

    def to_dict(self):
        return {
            "tx_id": self.tx_id,
            "parent_ids": self.parent_ids,
            "data": self.data,
            "signature": self.signature,
            "timestamp": self.timestamp
        }

    def compute_hash(self):
        tx_str = json.dumps(self.to_dict(), sort_keys=True)
        return sha256(tx_str.encode('utf-8')).hexdigest()

class DAGEngine:
    def _init_(self, persist_file="data/dag_ledger.json"):
        self.dag = nx.DiGraph()
        self.lock = threading.Lock()
        self.pending_transactions = {}  # Cache para tx pendentes
        self.persist_file = persist_file
        self.load_from_disk()

    def load_from_disk(self):
        """Carrega DAG e txs do arquivo de persistência, se existir."""
        if os.path.exists(self.persist_file):
            try:
                with open(self.persist_file, "r") as f:
                    data = json.load(f)
                for tx_data in data.get("transactions", []):
                    tx = Transaction(**tx_data)
                    self.dag.add_node(tx.tx_id, transaction=tx)
                    for pid in tx.parent_ids:
                        self.dag.add_edge(pid, tx.tx_id)
                print(f"DAG carregado de {self.persist_file} com {self.dag.number_of_nodes()} transações.")
            except Exception as e:
                print(f"Erro ao carregar DAG: {e}")

    def persist_to_disk(self):
        """Salva DAG e transações no arquivo."""
        with self.lock:
            data = {
                "transactions": [
                    self.dag.nodes[tx]['transaction'].to_dict()
                    for tx in self.dag.nodes
                ]
            }
            with open(self.persist_file, "w") as f:
                json.dump(data, f, indent=2)
            print(f"DAG persistido em {self.persist_file}.")

    def validate_transaction(self, tx: Transaction):
        """
        Validação simples:
         - Verifica se pais existem no DAG
         - Verifica se não cria ciclo
         - Simula validação de assinatura
        """
        with self.lock:
            # Verifica pais
            for pid in tx.parent_ids:
                if pid not in self.dag:
                    raise ValueError(f"Pai {pid} não encontrado no DAG.")
            # Verifica duplicado
            if tx.tx_id in self.dag:
                raise ValueError(f"Transação {tx.tx_id} já existe.")
            # Verifica ciclo hipotético
            self.dag.add_node(tx.tx_id, transaction=tx)
            for pid in tx.parent_ids:
                self.dag.add_edge(pid, tx.tx_id)
            if not nx.is_directed_acyclic_graph(self.dag):
                # Remove para não corromper
                self.dag.remove_node(tx.tx_id)
                raise ValueError("Adição da transação cria ciclo no DAG.")
            # Simula validação assinatura
            if not tx.signature or len(tx.signature) < 10:
                self.dag.remove_node(tx.tx_id)
                raise ValueError("Assinatura inválida ou ausente.")
            # Tudo ok, mantém o nó
            return True

    def add_transaction(self, tx: Transaction):
        """Adiciona transação ao DAG após validação."""
        with self.lock:
            self.validate_transaction(tx)
            # Transação já adicionada no validate_transaction
            self.persist_to_disk()
            print(f"Transação {tx.tx_id} adicionada com sucesso.")

    def get_transaction(self, tx_id):
        with self.lock:
            if tx_id not in self.dag:
                return None
            return self.dag.nodes[tx_id]['transaction']

    def get_parents(self, tx_id):
        with self.lock:
            if tx_id not in self.dag:
                return []
            return list(self.dag.predecessors(tx_id))

    def get_children(self, tx_id):
        with self.lock:
            if tx_id not in self.dag:
                return []
            return list(self.dag.successors(tx_id))

    def topological_sort(self):
        with self.lock:
            return list(nx.topological_sort(self.dag))

    def find_ancestors(self, tx_id):
        """Retorna todos os ancestrais de uma transação."""
        with self.lock:
            if tx_id not in self.dag:
                return []
            return list(nx.ancestors(self.dag, tx_id))

    def find_descendants(self, tx_id):
        """Retorna todos os descendentes de uma transação."""
        with self.lock:
            if tx_id not in self.dag:
                return []
            return list(nx.descendants(self.dag, tx_id))


# Exemplo básico de uso
if _name_ == "_main_":
    engine = DAGEngine()
    try:
        tx0 = Transaction(tx_id="tx0", signature="signature_valid_123456")
        engine.add_transaction(tx0)

        tx1 = Transaction(tx_id="tx1", parent_ids=["tx0"], signature="signature_valid_abcdef", data={"amount": 100})
        engine.add_transaction(tx1)

        tx2 = Transaction(tx_id="tx2", parent_ids=["tx0"], signature="signature_valid_ghijkl", data={"amount": 50})
        engine.add_transaction(tx2)

        tx3 = Transaction(tx_id="tx3", parent_ids=["tx1", "tx2"], signature="signature_valid_mnopqr", data={"amount": 75})
        engine.add_transaction(tx3)

        print("Ordem topológica atual do DAG:")
        print(engine.topological_sort())

        print("Ancestrais de tx3:")
        print(engine.find_ancestors("tx3"))

        print("Descendentes de tx0:")
        print(engine.find_descendants("tx0"))

    except Exception as e:
        print(f"Erro: {e}")