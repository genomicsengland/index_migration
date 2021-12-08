# need to be able to compare the ignored tables, take the approach of working
# out which columns can be used to order and then ordering the table before
# hashing the row and hashing the subset of the table
rm(list = objects())
options(stringsAsFactors = FALSE,
    scipen = 200)
library(dotenv)
library(DBI)

# change from A->B as needed
instance <- "B"

# this is a list of the tables that are being ignored by
# compare_two_db_copies_rc_hash.r, the final column being the columns to use
# to sort the db before hashing
tabs <- c(
'"db", "schema", "table", "order_by"',
'"covid_sd","gdppr","nov_2020", "nhs_number, date"',
'"covid_sd","gdppr","nov_2020_linked", "nhs_number, date"',
'"covid_sd","gdppr","oct_2020", "nhs_number, date"',
'"covid_sd","gdppr","oct_2020_linked", "nhs_number, date"',
'"covid_sd","gdppr","vw_oct_2020_ext_dedup2", "nhs_number, date"',
'"metrics","dq_report_results","test_result_archive", "test_result_id, test_de_datetime"',
'"metrics","project_close_down","flag_values", "id"',
'"metrics","research_release_clinical_data","rare_diseases_participant_phenotype", "participant_sk, hpo_id"',
'"secondary_data_clean","q1_19_nhsd","apc", "participant_id, epikey"',
'"secondary_data_clean","q1_19_nhsd","did", "submissiondataid"',
'"secondary_data_clean","q1_19_nhsd","did_bridge", "submissiondataid, participant_id"',
'"secondary_data_clean","q1_19_nhsd","op", "participant_id, attendkey"',
'"secondary_data_clean","q1_20_nhsd","apc", "participant_id, epikey"',
'"secondary_data_clean","q1_20_nhsd","did", "submissiondataid"',
'"secondary_data_clean","q1_20_nhsd","did_bridge", "submissiondataid, participant_id"',
'"secondary_data_clean","q1_20_nhsd","did_dedup", "submissiondataid"',
'"secondary_data_clean","q1_20_nhsd","op", "participant_id, attendkey"',
'"secondary_data_clean","q1_20_nhsd","rr11_did_dedup", "submissiondataid"',
'"secondary_data_clean","q2_18_nhsd","apc", "participant_id, epikey"',
'"secondary_data_clean","q2_18_nhsd","op1", "participant_id, attendkey"',
'"secondary_data_clean","q2_18_nhsd","op2", "participant_id, attendkey"',
'"secondary_data_clean","q2_18_nhsd","op3", "participant_id, attendkey"',
'"secondary_data_clean","q2_19_nhsd","apc", "participant_id, epikey"',
'"secondary_data_clean","q2_19_nhsd","did", "submissiondataid"',
'"secondary_data_clean","q2_19_nhsd","did_bridge", "submissiondataid, participant_id"',
'"secondary_data_clean","q2_19_nhsd","op", "participant_id, attendkey"',
'"secondary_data_clean","q3_18_nhsd","apc", "participant_id, epikey"',
'"secondary_data_clean","q3_18_nhsd","op", "participant_id, attendkey"',
'"secondary_data_clean","q4_18_nhsd","apc", "participant_id, epikey"',
'"secondary_data_clean","q4_18_nhsd","op", "participant_id, attendkey"',
'"secondary_data_clean","q4_19_nhsd","apc", "participant_id, epikey"',
'"secondary_data_clean","q4_19_nhsd","did", "submissiondataid"',
'"secondary_data_clean","q4_19_nhsd","did_bridge", "submissiondataid, participant_id"',
'"secondary_data_clean","q4_19_nhsd","op", "participant_id, attendkey"',
'"secondary_data_raw","public","q1_18_nhsd.op", "participant_id, attendkey"',
'"secondary_data_raw","q2_18_nhsd","apc", "study_id, epikey"',
'"secondary_data_raw","q2_18_nhsd","op", "study_id, attendkey"',
'"secondary_data_raw","q2_18_nhsd","opndq", "study_id, attendkey"',
'"secondary_data_raw","q3_17_nhsd","op", "participant_id, attendkey"',
'"secondary_data_raw","q3_18_nhsd","apc", "participant_id, epikey"',
'"secondary_data_raw","q4_17_nhsd","op", "participant_id, attendkey"'
)
d <- read.csv(text = paste0(tabs, collapse = "\n"))
d$row_count <- c(NA)
d$hash <- c(NA)

# generate the connection to given db in given instance
get_db_con <- function(db, instance_alias) {
    dbConnect(RPostgres::Postgres(),
              dbname = db,
              host = Sys.getenv(paste0(instance_alias, "_HOST")),
              port = Sys.getenv(paste0(instance_alias, "_PORT")),
              user = Sys.getenv(paste0(instance_alias, "_USER")),
              password = Sys.getenv(paste0(instance_alias, "_PWD")))
}

# get a hash for the whole table at a connection
get_table_hash <- function(con, schema, table, order_by) {
    cat(paste(">>>", paste0(schema, ".", table), "\n"))
    sql <- paste0('
        with d as (select md5(e::text) as hash from (select * from "',
        schema,
        '"."',
        table,
        '" d order by ',
        order_by,
        ' limit 100000) e ) select md5(array_agg(hash)::varchar) as h from d;'
    )
    d <- dbGetQuery(con, sql)$h
    return(d)
}
 
# get the row count for schema and table a connection. Needs the double quotes
# around schema and table as some don't play nice with DBI
get_row_count <- function(con, schema, table) {
    sql <- paste0('select count(*)::int from "', schema, '"."', table, '";')
    d <- dbGetQuery(con, sql)$count
    return(d)
}
 
dbs <- unique(d$db)

for (i in seq_along(dbs)) {
    cat(paste('---', dbs[i], '\n'))
    con <- get_db_con(dbs[i], instance)
    for (j in which(d$db == dbs[i])) {
        d$row_count[j] <- get_row_count(con, d$schema[j], d$table[j])
        d$hash[j] <- get_table_hash(con, d$schema[j], d$table[j], d$order_by[j])
    }
}

saveRDS(d, paste0("ignore_db_rc_hash_", instance, ".rds"))
