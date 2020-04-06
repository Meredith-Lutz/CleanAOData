library(RPostgreSQL)
library(chron)
library(stringr)
#library(data.table)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "p_verreauxi_2019_v1_3",
                 host = "localhost", port = 5433,
                 user = "postgres", password = "Animalbehavior1#")

focalData 	<- dbGetQuery(con, "SELECT * from main_tables.list_behaviors WHERE behavior = 'Scentmark'")

colnames(focalData)
scentmarks	<- focalData[,c('behavior_time', 'actor', 
	'food_tree', 'overmark', 'overmarked_whom', 'quadrat', 'tree_number', 'tree_species')]