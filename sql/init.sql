-- Snapshot diario idempotente: uma linha por data e categoria.
CREATE TABLE IF NOT EXISTS public.resumo_precos_categoria (
    data_referencia DATE NOT NULL,
    categoria VARCHAR(150) NOT NULL,
    quantidade_produtos INTEGER NOT NULL CHECK (quantidade_produtos >= 0),
    preco_medio NUMERIC(12, 2) NOT NULL,
    preco_minimo NUMERIC(12, 2) NOT NULL,
    preco_maximo NUMERIC(12, 2) NOT NULL,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_referencia, categoria)
);
