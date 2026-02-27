-- sql/21_dim_keyword_categoria_dedupe.sql
-- Rebuild final deduplicado desde staging (determinístico)

TRUNCATE analytics.dim_keyword_categoria;

INSERT INTO analytics.dim_keyword_categoria
  (keyword_raw,keyword_canonica,categoria,subcategoria,sinonimo_de,es_error_ortografico,prioridad)
SELECT DISTINCT ON (lower(keyword_raw))
  keyword_raw,
  keyword_canonica,
  categoria,
  subcategoria,
  sinonimo_de,
  COALESCE(es_error_ortografico,false),
  COALESCE(prioridad,2)
FROM analytics.stg_dim_keyword_categoria
WHERE keyword_raw IS NOT NULL
  AND keyword_canonica IS NOT NULL
  AND categoria IS NOT NULL
  AND subcategoria IS NOT NULL
ORDER BY
  lower(keyword_raw),
  COALESCE(prioridad,2) ASC,
  COALESCE(es_error_ortografico,false) ASC,
  (lower(keyword_raw)=lower(keyword_canonica)) DESC,
  length(keyword_canonica) DESC;