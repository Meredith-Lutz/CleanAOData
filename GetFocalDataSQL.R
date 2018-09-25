library(RPostgreSQL)
#library(data.table)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "diadema_pilot_2018_all_data",
                 host = "localhost", port = 5432,
                 user = "postgres", password = "Animalbehavior1#")

focalData 	<- dbGetQuery(con, "SELECT * from main_tables.all_focal_data_view_edited")
scanData	<- dbGetQuery(con, "SELECT * from main_tables.all_scan_data_view")

listFocals	<- unique(focalData$focal_start_time)

cleanedData	<- data.frame()
errorFile	<- data.frame()

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

test	<- seperateFocals(focalDataID, listFocals[2])
test2	<- seperateFocals(focalDataID, listFocals[180])[c(1, 4, 5:30),] #A file with (created) problems

cleanData	<- function(data, cleanFile, errorFile){
	### This function takes a dataset (data), an cleanedFile (can be empty), an error file (can be empty)
	### Returns as cleaned file and an error file with error codes
	### For behaviors without stop/start, these lines go straight to clean file (duration = 0)
	### For behaviors with start/stop, these lines are matched and then duration is calculated
	### If there is no match, then they go to an error file
	behaviors	<- unique(data$behavior)
	#print(behaviors)
	for(i in behaviors){
		sub	<- data[data$behavior == i,]
		#print(head(sub))
		if(sub$start_stop[1] == '1' | sub$start_stop[1] == 'Event'){ #Event
			#print(paste(i, 'is an event'))
			startTime	<- as.POSIXct(sub$behavior_time, format="%Y-%m-%d %H:%M:%S")
			#print(startTime)
			stopTime	<- as.POSIXct(sub$behavior_time, format="%Y-%m-%d %H:%M:%S")
			#print(stopTime)
			duration	<- stopTime - startTime
			cleanedChunk	<- cbind(sub, startTime, stopTime, duration)
			cleanFile	<- rbind(cleanFile, cleanedChunk)
			#print(cleanFile[,c('behavior_time', 'behavior', 'tree_number', 'dyadIDs', 'startTime', 'stopTime', 'duration')])

		}
		else{
			if(sub$actor[1] == sub$subject[1]){ #If the behavior isn't social, then its feeding, and we need to match trees
				trees	<- unique(sub$tree_number)
				#print(paste(i, 'was feeding'))
				for(j in trees){
					subTree	<- sub[sub$tree_number == j,] ##Just the data for that tree
					nStart		<- dim(subTree[subTree$start_stop == 'Start',])[1]
					nStop		<- dim(subTree[subTree$start_stop == 'Stop',])[1]
					if(nStart == nStop){ #Everything was started and stopped appropriately
						#print(paste(j, 'was started/stoppped appropriately'))
						subTree	<- subTree[order(subTree$behavior_time),]
						for(m in 1:(dim(subTree)[1]/2)){ #Take the start lines and use those, and add stop lines as stop time
							cleanedLine	<- subTree[c(2*m - 1),]
							startTime	<- as.POSIXct(subTree[c(2*m - 1),]$behavior_time, format="%Y-%m-%d %H:%M:%S")
							#print(startTime)
							stopTime	<- as.POSIXct(subTree[c(2*m),]$behavior_time, format="%Y-%m-%d %H:%M:%S")
							#print(stopTime)
							duration	<- stopTime - startTime
							cleanedLine	<- cbind(cleanedLine, startTime, stopTime, duration)
							cleanFile	<- rbind(cleanFile, cleanedLine)
						}
					}	
					if(nStart > nStop){
						#print(paste(j, 'has more starts than stops'))
						errorCode	<- rep(1, length = dim(subTree)[1])
						errorChunk	<- cbind(subTree, errorCode)
						errorFile	<- rbind(errorFile, errorChunk)
					}
					if(nStop > nStart){
						#print(paste(j, 'has more stops than starts'))
						errorCode	<- rep(2, length = dim(subTree)[1])
						errorChunk	<- cbind(subTree, errorCode)
						errorFile	<- rbind(errorFile, errorChunk)
					}
				}
			#print(cleanFile[,c('behavior_time', 'behavior', 'tree_number', 'dyadIDs', 'startTime', 'stopTime', 'duration')])
			}
			else{ #the behavior is social and dyads need to match
				dyads	<- unique(sub$dyadIDs)
				#print(paste(i, 'was social'))
				for(k in dyads){
					subDyad	<- sub[sub$dyadIDs == k,] #Just the data for that dyad and behav
					nStart	<- dim(subDyad[subDyad$start_stop == 'Start',])[1]
					nStop		<- dim(subDyad[subDyad$start_stop == 'Stop',])[1]
					if(nStart == nStop){ #Everything is started and stopped
						#print(paste(k, 'was started/stopped appropriately'))
						subDyad	<- subDyad[order(subDyad$behavior_time),]
						for(m in 1:(dim(subDyad)[1]/2)){
							cleanedLine	<- subDyad[c(2*m - 1),]
							startTime	<- as.POSIXct(subDyad[c(2*m - 1),]$behavior_time, format="%Y-%m-%d %H:%M:%S")
							#print(startTime)
							stopTime	<- as.POSIXct(subDyad[c(2*m),]$behavior_time, format="%Y-%m-%d %H:%M:%S")
							#print(stopTime)
							duration	<- stopTime - startTime
							cleanedLine	<- cbind(cleanedLine, startTime, stopTime, duration)
							cleanFile	<- rbind(cleanFile, cleanedLine)
						}
					}
					if(nStart > nStop){
						#print(paste(k, 'has more starts than stops'))
						errorCode	<- rep(1, length = dim(subDyad)[1])
						errorChunk	<- cbind(subDyad, errorCode)
						errorFile	<- rbind(errorFile, errorChunk)	
					}
					if(nStop > nStart){
						#print(paste(k, 'has more stops than starts'))
						errorCode	<- rep(2, length = dim(subDyad)[1])
						errorChunk	<- cbind(subDyad, errorCode)
						errorFile	<- rbind(errorFile, errorChunk)		
					}
				}
				#print(cleanFile[,c('behavior_time', 'behavior', 'tree_number', 'dyadIDs', 'startTime', 'stopTime', 'duration')])
			}
		}
	}
	return(list(cleanFile, errorFile))
}

cleanedTest	<- cleanData(test, cleanedData, errorFile)

cleanAllFocalData	<- function(data, cleanFile, errorFile){
	### This function takes in an entire database, seperates it out by focal, cleans each focal individually, and returns the clean and error files
	### Data is a dataset from AO (can contain multiple focals of data)
	### cleanFile is a blank data.frame
	### errorFile is a blank data.frame
	listFocals	<- unique(data$focal_start_time)
	for(i in 1:length(listFocals)){
		focalData	<- seperateFocals(data, listFocals[i])
		if(i == 1){
			output		<- cleanData(focalData, cleanFile, errorFile)
		}
		else{
			output		<- cleanData(focalData, output[1][[1]], output[2][[1]])
		}
	}
	return(output)
}

dbDisconnect(con)
dbUnloadDriver(drv)