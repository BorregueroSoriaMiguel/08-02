db.ventas.aggregate(
    [
        {
            $match: { 
                fecha_venta: { $gt: new Date ( "2021-01-01" ) }
            }
        },
        {
            $group:
            {
                _id: { 
                    codigo_pedido: "$cod",
                    producto: "$producto"
                },
                venta_total: { $sum: { $multiply: [ "$precio_ud_cliente", "$ud" ] } },
                rentabilidad: { $sum: { $divide: [ "$precio_ud_cliente", "$precio_ud_proveedor" ] } }
            }
        },
        {
            $project: {
                _id:0,
                codigo_pedido: "$_id.codigo_pedido",
                producto: "$_id.producto",
                rentable_porcentaje: { $trunc: [ { $multiply: [ "$rentabilidad",100 ] }, 1 ] },
                total_vendido_sin_IVA: "$venta_total",
                conseguido_solo_con_IVA: { $multiply: [ "$venta_total",0.21 ] },
                total_vendido_con_IVA: { $multiply: [ "$venta_total",1.21 ] },
                total_truncado_con_2_decimales: { $trunc: [ { $multiply: [ "$venta_total", 1.21 ] }, 2 ] },
                total_truncado_sin_decimal: { $trunc: [ { $multiply: [ "$venta_total", 1.21 ] }, 0 ] }
            }
        },
        {
            $sort: {
                codigo_pedido:1
            }
        },
        {
            $match: {
                $expr: { $gt: [ "$total_vendido_con_IVA", "$total_truncado_con_2_decimales" ] }
            }
        }
    ]
).pretty()