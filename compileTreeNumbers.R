#########################
##### Tree cleaning #####
#########################
setwd('G:/My Drive/Graduate School/Research/AO/CleanAOData/CleanAOData')

library(RPostgreSQL)

drv	<- dbDriver('PostgreSQL') ##Be sure to use real database name
con	<- dbConnect(drv, dbname = 'diadema_fulvus', host = 'localhost', port = 5432,
								 user = 'postgres', password = 'postgres')

diadema2LS	<- dbGetQuery(con, "select * from main_tables.list_sessions
					where group_id = 'Diadema 2';")
diadema3LS	<- dbGetQuery(con, "select * from main_tables.list_sessions
					where group_id = 'Diadema 3';")
fulvus2LS	<- dbGetQuery(con, "select * from main_tables.list_sessions
					where group_id = 'Fulvus 2';")
fulvus3LS	<- dbGetQuery(con, "select * from main_tables.list_sessions
					where group_id = 'Fulvus 3';")

if (condition1) { 
    expr1
    } else if (condition2) {
    expr2
    } else if  (condition3) {
    expr3
    } else {
    expr4
}

findStartEndTrees	<- function(list_sessions){
	nSessions	<- dim(list_sessions)[1]
	group		<- list_sessions[1, 'group_id']
	groupAbb	<- if(group == 'Diadema 2'){
				'D2'} else if(group == 'Diadema 3'){
				'D3'} else if(group == 'Fulvus 2'){
				'F2'} else if(group == 'Fulvus 3'){
				'F3'}

	drv	<- dbDriver('PostgreSQL') ##Be sure to use real database name
	con	<- dbConnect(drv, dbname = 'diadema_fulvus', host = 'localhost', port = 5432,
			 user = 'postgres', password = 'postgres')

	behav_data	<- dbGetQuery(con, "select * from main_tables.list_behaviors
						left join main_tables.list_focals
						on main_tables.list_behaviors.focal_start_time = main_tables.list_focals.focal_start_time;")
	
	treeMat	<- data.frame(session_start_time = NA,
					group = NA,
					groupAbbreviation = NA,
					minTree = NA,
					maxTree = NA)

	for(i in 1:nSessions){
		sessionTrees	<- behav_data[behav_data$session_start_time == list_sessions[i,2], "tree_number"]
		groupTrees		<- sessionTrees[substr(sessionTrees, 1, 2) == groupAbb]
		minTree		<- min(groupTrees, na.rm = TRUE)
		maxTree		<- max(groupTrees, na.rm = TRUE)
		row			<- c(as.character(list_sessions[i,2]), group, groupAbb, minTree, maxTree)
		treeMat		<- rbind(treeMat, row)
	}
	return(treeMat)
}

sessions_all	<- list(diadema2LS, diadema3LS, fulvus2LS, fulvus3LS)
listTreesAll	<- lapply(sessions_all, findStartEndTrees)

write.csv(listTreesAll[1], 'diadema2TreeStartStops.csv')
write.csv(listTreesAll[2], 'diadema3TreeStartStops.csv')
write.csv(listTreesAll[3], 'fulvus2TreeStartStops.csv')
write.csv(listTreesAll[4], 'fulvus3TreeStartStops.csv')



