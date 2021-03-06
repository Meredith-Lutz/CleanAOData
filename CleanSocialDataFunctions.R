seperateFocals	<- function(data, startTime){
	### This function breaks a data.frame up into seperate data sets for each focal
	### data is a data.frame of inputted data
	### startTime is the start time of the focal
	### returns a data.frame from just that day
	return(data[data$focal_start_chron == startTime,])
}

createID	<- function(vector){
	### This function alphabetizes the actor and subject, and then pastes them together
	### vector is a vector of only the actor and subject
	id	<- paste(sort(vector), collapse = '')
	return(id)
}

cleanData	<- function(data, cleanFile, errorFile){
	### This function takes a dataset (data), an cleanedFile (can be empty), an error file (can be empty)
	### Returns as cleaned file and an error file with error codes
	### For behaviors without stop/start, these lines go straight to clean file (duration = 0)
	### For behaviors with start/stop, these lines are matched and then duration is calculated
	### If there is no match, then they go to an error file
	dyads		<- unique(data$dyadIDs)
	print(dyads)
	for(i in dyads){
		sub	<- data[data$dyadIDs == i,]
		#print(head(sub))
		behaviors		<- unique(sub$behavior)
		for(k in behaviors){
			subBehav	<- sub[sub$behavior == k,]
			#print(dim(subBehav)
			if(is.na(subBehav$start_stop[1]) == TRUE){ #Event
				print(paste(i, 'is an event'))
				startTime		<- subBehav$behavior_time_chron
				#print(startTime)
				stopTime		<- subBehav$behavior_time_chron
				#print(stopTime)
				duration		<- as.numeric(stopTime - startTime)*24*60*60
				cleanedChunk	<- cbind(subBehav, startTime, stopTime, duration)
				cleanFile		<- rbind(cleanFile, cleanedChunk)
				#print(cleanFile[,c('behavior_time', 'behavior', 'tree_number', 'dyadIDs', 'startTime', 'stopTime', 'duration')])
			}
			else{
				nStart	<- dim(subBehav[subBehav$start_stop == 'Start',])[1]
				print(nStart)
				nStop		<- dim(subBehav[subBehav$start_stop == 'Stop',])[1]
				print(nStop)
				if(nStart == nStop){ #Everything is started and stopped
					print(paste(i, 'was started/stopped appropriately for ', k))
					subBehav	<- subBehav[order(subBehav$behavior_time),]
					for(m in 1:(dim(subBehav)[1]/2)){
						cleanedLine	<- subBehav[c(2*m - 1),]
						startTime	<- subBehav[c(2*m - 1),]$behavior_time_chron
						#print(startTime)
						stopTime	<- subBehav[c(2*m),]$behavior_time_chron
						#print(stopTime)
						duration	<- as.numeric(stopTime - startTime)*24*60*60
						cleanedLine	<- cbind(cleanedLine, startTime, stopTime, duration)
						cleanFile	<- rbind(cleanFile, cleanedLine)
					}
				}
				if(nStart > nStop){
					print(paste(i, 'has more starts than stops for ', k))
					errorCode	<- rep(1, length = dim(subBehav)[1])
					errorChunk	<- cbind(subBehav, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)	
				}
				if(nStop > nStart){
					print(paste(i, 'has more stops than starts for ', k))
					errorCode	<- rep(2, length = dim(subBehav)[1])
					errorChunk	<- cbind(subBehav, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)		
				}
			} #close else
		} # close behavior loop
	} #close dyad loop
	#print(cleanFile[,c('behavior_time', 'behavior', 'tree_number', 'dyadIDs', 'startTime', 'stopTime', 'duration')])
	return(list(cleanFile, errorFile))
}

cleanAllFocalData	<- function(data, cleanFile, errorFile){
	### This function takes in an entire database, seperates it out by focal, cleans each focal individually, and returns the clean and error files
	### Data is a dataset from AO (can contain multiple focals of data)
	### cleanFile is a blank data.frame
	### errorFile is a blank data.frame
	listFocals	<- unique(data$focal_start_chron)
	for(i in 1:length(listFocals)){
		print(paste('Beginning to clean', listFocals[i]))
		focalData	<- seperateFocals(data, listFocals[i])
		print(paste(dim(focalData)[1], 'lines of data'))
		if(i == 1){
			output		<- cleanData(focalData, cleanFile, errorFile)
		}
		else{
			output		<- cleanData(focalData, output[[1]], output[[2]])
		}
		print(paste(listFocals[i], 'has been cleaned'))
	}
	return(output)
}