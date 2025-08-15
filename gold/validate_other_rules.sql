-- Este query calcula el ranking de ventas por cliente

SELECT
  cliente_id,
  venta_total,
  -- ESTE ES UN USO CORRECTO de ORDER BY dentro de una función de ventana.
  -- El linter NO debería marcar esto como un error.
  ROW_NUMBER() OVER (PARTITION BY region ORDER BY venta_total DESC) as ranking_regional

FROM
  dev.silver.ventas_agregadas

-- ESTE ES UN USO INCORRECTO de ORDER BY que ordena todo el resultado final.
-- El linter SÍ debería marcar esta línea como un error.
ORDER BY
  prod.venta_total DESC;