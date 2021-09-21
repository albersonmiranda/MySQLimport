#' Importar arquivo de transações monetárias de cartões Visa
#'
#' Importa o arquivo ARQTRA para o R ou para o MySQL.
#'
#' @export
#'
#' @param caminho String. O caminho do arquivo a ser importado, incluindo a
#'   extensão (.txt).
#' @param MySQL Logical. Se TRUE, entao o arquivo será importado para o MySQL.
#' @param schema String. Se MySQL = TRUE, o nome do schema que conterá a tabela
#'   ARQTRA no MySQL. O schema deve existir no MySQL antes de executar a função.
#' @param tabela String. Se MySQL = TRUE, o nome da tabela a ser criada no
#'   servidor que conterá os dados importados.
#'
#' @details Se for importar apenas para o R, lembre-se de nomear um objeto para
#'   o resultado da função. Caso for importar para o MySQL, não é necessário
#'   criar um objeto. Note que a tabela será sobrescrita se já existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_gco("C:\\Arquivos\\GCO\\202009.txt")
#'
#' # para importar para o MySQL:
#' import_gco(
#'     "C:\\Arquivos\\GCO\\202009.txt",
#'     mysql = TRUE,
#'     tabela = "t202009",
#'     user = "alberson"
#' )
#' }
#'
import_arqtra = function(caminho,
                         mysql = FALSE,
                         schema = "ARQTRA",
                         tabela,
                         drop = FALSE,
                         metodo = c("vroom", "readr"),
                         host,
                         port,
                         user,
                         password = NULL) {

    # evaluate arg tabela
    if (mysql == TRUE && is.null(tabela)) {
        stop("tabela deve ser informada")
    }

    # evaluate arg host
    if (mysql == TRUE && is.null(user)) {
        stop("host deve ser informado")
    }

    # evaluate arg user
    if (mysql == TRUE && is.null(user)) {
        stop("user deve ser informado")
    }

    # evaluate arg metodo
    match.arg(metodo)

    # conexao com MySQL
    if (mysql == TRUE) {
        tryCatch(
            {
                con = DBI::dbConnect(RMySQL::MySQL(),
                    host = host,
                    port = port,
                    db = schema,
                    user = user,
                    password = ifelse(
                        is.null(password),
                        rstudioapi::askForPassword("Database password"),
                        password
                    )
                )
            },
            error = function(cond) {
                message(cond)
                stop(
                    paste(
                        "Talvez você não tenha criado o schema",
                        schema,
                        "no MySQL?"
                    )
                )
            }
        )
    }

    # leitura arquivo
    larguras = c(
        1, 19, 5, 8, 8, 3, 11, 23, 40, 20, 2, 1, 2, 13, 2, 5, 2, 2, 3, 5,
        5, 9, 1, 1, 3, 1, 1, 10
    )

    colunas = c(
        "tipo", "nr_conta", "nr_plastico", "dt_postagem",
        "dt_transacao", "cod_transacao", "vlr_transacao",
        "nr_intercambio", "descricao", "nome_estab", "nr_parcela",
        "barra", "total_parcela_cpp", "cidade", "estado", "nr_plano",
        "total_parcela", "rejeicao", "fonte_cms", "fonte_trams",
        "cat_estab", "vlr_dolar", "tp_transacao", "carac_transacao",
        "nr_componente", "status_cartao", "cod_bloqueio", "filler_1"
    )

    col_types = readr::cols(
        tipo = readr::col_character(),
        dt_postagem = readr::col_date("%d%m%Y"),
        dt_transacao = readr::col_date("%d%m%Y"),
        vlr_transacao = readr::col_double()
    )

    # importar arquivo para o R via {readr}
    if (metodo == "readr") {
        data = readr::read_fwf(
            caminho,
            readr::fwf_widths(
                larguras,
                colunas,
            ),
            col_types = col_types
        )
    }

    # importar arquivo para o R via {vroom}
    if (metodo == "vroom") {
        data = vroom::vroom_fwf(
            caminho,
            readr::fwf_widths(
                larguras,
                colunas
            ),
            col_types = col_types
        )
    }

    # criando coluna produto
    data$produto = substr(data$nr_conta, 4, 7)

    # removendo header e trailler
    data = data[data$tipo == 1, ]

    # criar tabela no MySQL
    if (mysql == TRUE) {

        # sobrescrever tabela
        if (drop == TRUE) {

            # dropar tabela
            DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
            # gravar tabela
            DBI::dbWriteTable(con, DBI::SQL(tabela), data)
            # gravar índices
            DBI::dbSendQuery(con, paste0("
            ALTER TABLE ", schema, ".", tabela, "
            ADD INDEX idx_produto (produto(4));
            "))

        } else {

            # append da tabela
            DBI::dbWriteTable(con, DBI::SQL(tabela), data, append = TRUE)
        }


        # desconectando
        DBI::dbDisconnect(con)
    } else {
        data
    }
}