setwd('G:/My Drive/Graduate School/Research/Projects/ReconciliationSDB')

library(RPostgreSQL)
library(sp)
library(rgdal)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "diadema_fulvus",
                 host = "localhost", port = 5432,
                 user = "postgres", password = "postgres")

focalData	<- dbGetQuery(con, "SELECT * from main_tables.all_focal_data_view")

scanDataSimp	<- scanData[scanData$focal_individual_id == scanData$scanned_individual_id,c('group_id', 'focal_start_time', 'scan_time', 'scanned_individual_id',
				'activity', 'height', 'x_position', 'y_position', 'latitude', 'longitude', 'compass_bearing',
				'quadrat', 'distance_to_group')]

listFocals	<- dbGetQuery(con, "SELECT * from main_tables.list_focals
					left join main_tables.list_sessions
					on main_tables.list_sessions.session_start_time = main_tables.list_focals.session_start_time")

listFocalsSimp	<- listFocals[,c(1:4, 7, 15:16)]

alarms	<- dbGetQuery(con, "SELECT * from main_tables.all_focal_data_view
					where main_tables.all_focal_data_view.behavior = 'Alarm call'")

alarmsSimp	<- alarms[,c(1:10, 20, 23:27)]

alarmsSub	<- alarmsSimp[alarmsSimp$pin_code_name == 'Meredith' | alarmsSimp$pin_code_name == 'Hasina',]

write.csv(alarmsSub, 'alarmCalls.csv')
write.csv(listFocalsSimp, 'listFocals.csv')

agg	<- dbGetQuery(con, "SELECT * from main_tables.all_focal_data_view
					where main_tables.all_focal_data_view.category = 'Aggressive' or main_tables.all_focal_data_view.category = 'Submissive'")

aggSub	<- agg[agg$pin_code_name == 'Meredith' | agg$pin_code_name == 'Hasina',]

aggSimp	<- aggSub[,c(1:14, 23:27)]
write.csv(aggSimp, 'aggressionSubmission.csv')
