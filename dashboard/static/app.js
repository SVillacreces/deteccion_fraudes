// ==================================================
// TRANSACCIONES
// ==================================================

async function cargarTransacciones() {

    try {

        const respuesta =
            await fetch("/api/transacciones");


        if (!respuesta.ok) {

            throw new Error(
                "Error consultando transacciones"
            );

        }


        const datos =
            await respuesta.json();


        const tabla =
            document.getElementById(
                "tabla-validaciones"
            );


        tabla.innerHTML = "";


        if (
            !datos.transacciones
            || datos.transacciones.length === 0
        ) {

            tabla.innerHTML = `
                <tr>
                    <td colspan="7">
                        No existen transacciones disponibles.
                    </td>
                </tr>
            `;

            return;
        }


        datos.transacciones.forEach(
            transaccion => {

                const fila =
                    document.createElement(
                        "tr"
                    );


                fila.innerHTML = `

                    <td>
                        ${transaccion.transaction_id}
                    </td>

                    <td>
                        $${Number(
                            transaccion.amount
                        ).toFixed(2)}
                    </td>

                    <td>
                        ${transaccion.transaction_frequency}
                    </td>

                    <td>
                        ${transaccion.expected_status}
                    </td>

                    <td>
                        ${transaccion.detected_status}
                    </td>

                    <td>
                        ${transaccion.evaluation}
                    </td>

                    <td>
                        ${transaccion.latency_ms} ms
                    </td>

                `;


                tabla.appendChild(
                    fila
                );

            }
        );


    } catch (error) {

        console.error(
            "Error cargando transacciones:",
            error
        );


        const tabla =
            document.getElementById(
                "tabla-validaciones"
            );


        if (tabla) {

            tabla.innerHTML = `
                <tr>
                    <td colspan="7">
                        Error cargando transacciones.
                    </td>
                </tr>
            `;

        }

    }

}



// ==================================================
// MÉTRICAS DE DETECCIÓN
// ==================================================

async function cargarMetricas() {

    try {

        const respuesta =
            await fetch("/api/metricas");


        if (!respuesta.ok) {

            throw new Error(
                "Error consultando métricas"
            );

        }


        const datos =
            await respuesta.json();


        document.getElementById(
            "porcentaje-anomalias"
        ).textContent =
            Number(
                datos.porcentaje_anomalias
            ).toFixed(2);


        document.getElementById(
            "f1-score"
        ).textContent =
            Number(
                datos.f1_score
            ).toFixed(4);


        document.getElementById(
            "tp"
        ).textContent =
            datos.matriz_confusion.tp;


        document.getElementById(
            "tn"
        ).textContent =
            datos.matriz_confusion.tn;


        document.getElementById(
            "fp"
        ).textContent =
            datos.matriz_confusion.fp;


        document.getElementById(
            "fn"
        ).textContent =
            datos.matriz_confusion.fn;


        document.getElementById(
            "precision"
        ).textContent =
            Number(
                datos.precision
            ).toFixed(2)
            + "%";


        document.getElementById(
            "recall"
        ).textContent =
            Number(
                datos.recall
            ).toFixed(2)
            + "%";


        document.getElementById(
            "fpr"
        ).textContent =
            Number(
                datos.false_positive_rate
            ).toFixed(2)
            + "%";


        document.getElementById(
            "total-evaluados"
        ).textContent =
            datos.total;


    } catch (error) {

        console.error(
            "Error cargando métricas:",
            error
        );

    }

}



// ==================================================
// MÉTRICAS DE RENDIMIENTO
// ==================================================

async function cargarRendimiento() {

    try {

        const respuesta =
            await fetch(
                "/api/rendimiento"
            );


        if (!respuesta.ok) {

            throw new Error(
                "Error consultando rendimiento"
            );

        }


        const datos =
            await respuesta.json();


        document.getElementById(
            "rendimiento"
        ).textContent =
            Number(
                datos.tps
            ).toFixed(2);


        document.getElementById(
            "latencia-promedio"
        ).textContent =
            Number(
                datos.latencia_promedio_ms
            ).toFixed(2);


        document.getElementById(
            "latencia-minima"
        ).textContent =
            datos.latencia_minima_ms;


        document.getElementById(
            "latencia-maxima"
        ).textContent =
            datos.latencia_maxima_ms;


        document.getElementById(
            "latencia-p95"
        ).textContent =
            datos.latencia_p95_ms;


        document.getElementById(
            "eventos-ventana"
        ).textContent =
            datos.total_eventos_ventana;


    } catch (error) {

        console.error(
            "Error cargando rendimiento:",
            error
        );

    }

}



// ==================================================
// ESTADO DE INFRAESTRUCTURA
// ==================================================

async function cargarInfraestructura() {

    try {

        const respuesta =
            await fetch(
                "/api/infraestructura"
            );


        if (!respuesta.ok) {

            throw new Error(
                "Error consultando infraestructura"
            );

        }


        const datos =
            await respuesta.json();


        actualizarEstadoComponente(
            "estado-kafka",
            datos.componentes.kafka.estado
        );


        actualizarEstadoComponente(
            "estado-flink",
            datos.componentes.flink.estado
        );


        actualizarEstadoComponente(
            "estado-cassandra",
            datos.componentes.cassandra.estado
        );


        actualizarEstadoComponente(
            "estado-api",
            datos.componentes.api.estado
        );


    } catch (error) {

        console.error(
            "Error cargando infraestructura:",
            error
        );


        actualizarEstadoComponente(
            "estado-kafka",
            "INACTIVO"
        );


        actualizarEstadoComponente(
            "estado-flink",
            "INACTIVO"
        );


        actualizarEstadoComponente(
            "estado-cassandra",
            "INACTIVO"
        );


        actualizarEstadoComponente(
            "estado-api",
            "INACTIVO"
        );

    }

}



// ==================================================
// ACTUALIZAR ESTADO VISUAL
// ==================================================

function actualizarEstadoComponente(
    idElemento,
    estado
) {

    const elemento =
        document.getElementById(
            idElemento
        );


    if (!elemento) {

        return;
    }


    if (estado === "ACTIVO") {

        elemento.textContent =
            "Activo";


        elemento.classList.remove(
            "pendiente"
        );


        elemento.classList.remove(
            "inactivo"
        );


        elemento.classList.add(
            "activo"
        );

    } else {

        elemento.textContent =
            "Inactivo";


        elemento.classList.remove(
            "pendiente"
        );


        elemento.classList.remove(
            "activo"
        );


        elemento.classList.add(
            "inactivo"
        );

    }

}



// ==================================================
// PRIMERA CARGA
// ==================================================

cargarTransacciones();

cargarMetricas();

cargarRendimiento();

cargarInfraestructura();



// ==================================================
// ACTUALIZACIÓN AUTOMÁTICA
// ==================================================

setInterval(
    cargarTransacciones,
    2000
);


setInterval(
    cargarMetricas,
    2000
);


setInterval(
    cargarRendimiento,
    2000
);


setInterval(
    cargarInfraestructura,
    5000
);