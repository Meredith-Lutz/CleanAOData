library(RPostgreSQL)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "Diadema2018",
                 host = "localhost", port = 5432,
                 user = "postgres", password = "Animalbehavior1#")

focalData 	<- dbGetQuery(con, "SELECT * from main_tables.all_focal_data_view")
scanData	<- dbGetQuery(con, "SELECT * from main_tables.all_scan_data_view")

listFocals	<- unique(focalData$focal_start_time)

cleanedData	<- data.table()


seperateFocals	<- function(data, startTime){
	### This function breaks a data.frame up into seperate data sets for each focal
	### data is a data.frame of inputted data
	### startTime is the start time of the focal
	### returns a data.frame from just that day
	return(data[data$focal_start_time == startTime,])
}

createID	<- function(vector){
	### This function alphabetizes the actor and subject, and then pastes them together
	### vector is a vector of only the actor and subject
	id	<- paste(sort(vector), collapse = '')
	return(id)
}

dyadIDs	<- apply(as.matrix(focalData[, c('actor', 'subject')]), 1, createID)
focalDataID	<- cbind(focalData, dyadIDs)

cleanData	<- function(data, cleanFile, errorFile){
	### This function takes a dataset (data), an cleanedFile (can be empty), an error file (can be empty)
	### Returns as cleaned file and an error file with error codes
	### For behaviors without stop/start, these lines go straight to clean file (duration = 0)
	### For behaviors with start/stop, these lines are matched and then duration is calculated
	### If there is no match, then they go to an error file
	behaviors	<- unique(data$behavior)
	for(i in behaviors){
		sub	<- data[data$behavior == behaviors[i],]
		if(sub$behavior[1] != 'Start' && sub$behavior[1] != 'Stop'){ #Event
			cleanFile	<- rbind(cleanFile, sub)
		}
		else{
			if(sub$actor[1] == sub$subject[1]){ #If the behavior isn't social, then its feeding, and we need to match trees
				trees	<- unique(sub$tree_number)
				for(j in trees){
					subTree	<- sub[sub$tree_number == trees[j],] ##Just the data for that tree
					nStart	<- dim(subTree[subTree$tree_number == 'Start',])[1]
					nStop		<- dim(subTree[subTree$tree_number == 'Stop',])[1]
					if(nStart == nStop){ #Everything was started and stopped appropriately
						#take the 1st start and the 1st stop and calc duration
						#data line to append is created from start + duration
						#append data line
					}	
					if(nStart > nStop){
						#write everything to error file with error code 01: too many starts
					}
					if(nStop > nStart){
						#write everything to error file with error code 02: too many stops
					}
				}
			}
			else{ #the behavior is social and dyads need to match
				dyads	<- unique(sub$dyadIDs)
				for(k in dyads){
					subDyad	<- sub[sub$dyadIDs == dyads[k],] #Just the data for that dyad and behav
					nStart	<- dim(subDyad[subDyad$start_stop == 'Start',])[1]
					nStop		<- dim(subDyad[subDyad$start_stop == 'Stop',])[1]
					if(nStart == nStop){ #Everything is started and stopped
						#take the 1st start and the 1st stop and calc duration
						#data line to append is created from start + duration
						#append data line
					}
					if(nStart > nStop){
						#write everything to error file with error code 01: too many starts
					}
					if(nStop > nStart){
						#write everything to error file with error code 02: too many stops
					}
				}
			}
		}
	}
	return(list(cleanFile, errorFile))
}
			