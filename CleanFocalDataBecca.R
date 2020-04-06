library(RPostgreSQL)
library(chron)
library(stringr)
#library(data.table)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "p_verreauxi_2019_v1_3",
                 host = "localhost", port = 5433,
                 user = "postgres", password = "Animalbehavior1#")

focalData 	<- dbGetQuery(con, "SELECT * from main_tables.all_focal_data_view")
scanData	<- dbGetQuery(con, "SELECT * from main_tables.all_scan_data_view")

focal_start_str	<- data.frame(str_split_fixed(as.character(focalData$focal_start_time), ' ', n = 2))
colnames(focal_start_str)	<- c('focal_date', 'focal_time')
focal_start_chron	<- chron(dates. = as.character(focal_start_str$focal_date), times. = as.character(focal_start_str$focal_time),
	format = c(dates = 'y-m-d', times = 'h:m:s'))
behavior_time_str	<- data.frame(str_split_fixed(as.character(focalData$behavior_time), ' ', n = 2))
colnames(behavior_time_str)	<- c('behav_date', 'behav_time')
behavior_time_chron	<- chron(dates. = as.character(behavior_time_str$behav_date), times. = as.character(behavior_time_str$behav_time),
	format = c(dates = 'y-m-d', times = 'h:m:s'))
focalData$focal_start_chron	<- focal_start_chron
focalData$behavior_time_chron	<- behavior_time_chron

cleanedData	<- data.frame()
errorFile	<- data.frame()

seperateFocals	<- function(data, startTime){
	### This function breaks a data.frame up into seperate data sets for each focal
	### data is a data.frame of inputted data
	### startTime is the start time of the focal
	### returns a data.frame from just that focal
	return(data[data$focal_start_time == startTime,]) #Fix this
}

createID	<- function(vector){
	### This function alphabetizes the actor and subject, and then pastes them together
	### vector is a vector of only the actor and subject
	id	<- paste(sort(vector), collapse = '')
	return(id)
}

dyadIDs	<- apply(as.matrix(focalData[, c('actor', 'subject')]), 1, createID)
focalDataID	<- cbind(focalData, dyadIDs)

focalDataNoNA	<- focalDataID[is.na(focalDataID$actor) == FALSE,]

cleanData	<- function(data, cleanFile, errorFile, byHand){
	### This function takes a dataset (data), an cleanedFile (can be empty), an error file (can be empty)
	### Returns as cleaned file and an error file with error codes
	### For behaviors without stop/start, these lines go straight to clean file (duration = 0)
	### For behaviors with start/stop, these lines are matched and then duration is calculated
	### If there is no match, then they go to an error file
	print(dim(cleanFile))
	print(dim(errorFile))
	print(dim(byHand))
	behaviors	<- unique(data$behavior)
	durBehaviors	<- c('Greet', 'Groom', 'Mutual Groom', 'Invite to groom', 'Play', 'Clasp', 'Mount', 'Chase', 'Chatter', 'Fight', 'In contact', 'Within 1m')
	matchBehaviors	<- c('Approach 1m', 'Approach Contact', 'Withdraw 1m', 'Withdraw >1m')
	events	<- c('Bite', 'Cuff', 'Feign to cuff', 'Food Rob', 'Grab', 'Head toss', 'Lunge', 'Nose jab', 'Snap at', 'Supplant', 'Flee <2m', 'Flee >2m', 'Grimace', 'Tail curl')
	appWith	<- data.frame()
	print('Cleaning')
	for(i in behaviors){
		sub	<- data[data$behavior == i,]
		print(i)
		if(i == 'Within 1m' | i == 'In contact'){ #Pull the withdraw lines from all within contacts
			endTimes	<- sub[sub$start_stop == 'Stop', c('behavior_time')]
			#print(endTimes)
			focalWithdraws	<- data[which(data$behavior == 'Withdraw 1m'| data$behavior == 'Withdraw >1m'),]
			allWith		<- data.frame()
			for(q in endTimes){
				withdraw1	<- data[data$behavior_time == q & data$behavior == 'Withdraw 1m',]
				withdrawG1 	<- data[which(data$behavior_time == q & data$behavior == 'Withdraw >1m'),]
				withdraws	<- rbind(withdraw1, withdrawG1)
			#	print(dim(withdraws))
			#	print(colnames(withdraws))
				allWith	<- rbind(allWith, withdraws)
			}
			#print(length(endTimes))
			#print(dim(allWith))
			if(length(endTimes) != dim(allWith)[1]){
				errorChunk	<- rbind(sub, withdraws)
				errorCode	<- rep(12, dim(errorChunk)[1])
				errorChunk	<- cbind(errorChunk, errorCode)
				errorFile	<- rbind(errorFile, errorChunk)
			}
			if(length(endTimes) == dim(allWith)[1]){
			#	print(dim(sub))
				#print(colnames(sub))
				#print(colnames(allWith))
				sub	<- rbind(sub, allWith)
			}
		}
	#	print(head(sub))
		if(sub[1,]$behavior %in% events){ #Event
			print(paste(i, 'is an event'))
			startTime	<- sub$behavior_time ##Fix this line
			#print(startTime)
			stopTime	<- sub$behavior_time ##Fix this line
			#print(stopTime)
			#duration	<- as.numeric(stopTime - startTime)*24*60*60
			cleanedChunk	<- cbind(sub, startTime, stopTime)
			cleanFile	<- rbind(cleanFile, cleanedChunk)
			#print(cleanFile[,c('behavior_time', 'behavior', 'tree_number', 'dyadIDs', 'startTime', 'stopTime', 'duration')])

		}
		if(sub[1,]$behavior %in% durBehaviors){ #Behavior already has start stops, and just needs to be matched
			dyads	<- unique(sub$dyadIDs)
			print(paste(i, 'was social'))
			for(k in dyads){
				subDyad	<- sub[sub$dyadIDs == k,] #Just the data for that dyad and behav
				nStart	<- dim(subDyad[subDyad$start_stop == 'Start',])[1]
				nStop		<- dim(subDyad[subDyad$start_stop == 'Stop',])[1]
				if(nStart == nStop){ #Everything is started and stopped
					print(paste(k, 'was started/stopped appropriately'))
					withdraws2	<- subDyad[which(subDyad$behavior == 'Withdraw 1m' | subDyad$behavior == 'Withdraw >1m'),]
					withdraws2$startTime	<- withdraws2$behavior_time
					withdraws2$stopTime	<- withdraws2$behavior_time
					subBehav	<- subDyad[which(subDyad$behavior == i),]
					subBehav	<- subBehav[order(subBehav$behavior_time),]
					#print(dim(subBehav))
					#print(subBehav)
					for(m in 1:(dim(subBehav)[1]/2)){
						cleanedLine	<- subBehav[c(2*m - 1),]
						startTime	<- subBehav[c(2*m - 1),]$behavior_time ##Fix these lines
						#print(startTime)
						stopTime	<- subBehav[c(2*m),]$behavior_time ##Fix these lines
						#print(stopTime)
						#duration	<- as.numeric(stopTime - startTime)*24*60*60
						cleanedLine	<- cbind(cleanedLine, startTime, stopTime) #Fix
						#print(dim(cleanedLine))
						#print(dim(cleanFile))
						#print(colnames(cleanFile))
						cleanFile	<- rbind(cleanFile, cleanedLine)
					}
					if(i == 'Within 1m'| i == 'In contact'){
						cleanFile	<- rbind(cleanFile, withdraws2)
					}
				}
				if(nStart > nStop){
					print(paste(k, 'has more starts than stops'))
					errorCode	<- rep(1, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)	
				}
				if(nStop > nStart){
					print(paste(k, 'has more stops than starts'))
					errorCode	<- rep(2, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)		
				}
			}
			#print(cleanFile[,c('behavior_time', 'behavior', 'tree_number', 'dyadIDs', 'startTime', 'stopTime', 'duration')])
		}
		if(sub[1,]$behavior %in% matchBehaviors){#behavior was an approach/withdraw situation
			appWith	<- rbind(appWith, sub)
		}
	}
	
	print('finished cleaning all but approaches and withdraws')

	dyads	<- unique(appWith$dyadIDs) #break it down by dyads, NEED TO BE ABOUT WITH
	for(d in dyads){
	#	print(dyads)
		print(d)
		subset	<- appWith[appWith$dyadIDs == d,]
	#	print(dim(subset))
		app1		<- subset[subset$behavior == 'Approach 1m',]
		appcnt	<- subset[subset$behavior == 'Approach Contact',]
		with1		<- subset[subset$behavior == 'Withdraw 1m',]
		withG1	<- subset[subset$behavior == 'Withdraw >1m',]
	#	print(dim(app1))
	#	print(dim(appcnt))
	#	print(dim(with1))
	#	print(dim(withG1))
		if(dim(appcnt)[1] == 0 & dim(with1)[1] == 0){ #There were no contacts
			if(dim(app1)[1] == dim(withG1)[1]){ #Withdraws and approaches line up
				print('no contacts approach and withdraw line up')
				subDyad	<- rbind(app1, withG1)
				subDyad	<- subDyad[order(subDyad$behavior_time),]
			for(m in 1:(dim(subDyad)[1]/2)){
					cleanedLine	<- subDyad[c(2*m - 1, 2*m),]
					startTime	<- subDyad[c(2*m - 1, 2*m),]$behavior_time ##Fix these lines
					#print(startTime)
					stopTime	<- rep(subDyad[c(2*m),]$behavior_time) ##Fix these lines
					#print(stopTime)
					#duration	<- as.numeric(stopTime - startTime)*24*60*60
					cleanedLine	<- cbind(cleanedLine, startTime, stopTime) #Fix
					cleanFile	<- rbind(cleanFile, cleanedLine)
				}
			}
			if(dim(app1)[1] > dim(withG1)[1]){ #More approaches than withdraws
				print('more approaches than withdraws')
				subDyad	<- rbind(app1, withG1)
				subDyad	<- subDyad[order(subDyad$behavior_time),]
				errorCode	<- rep(1, length = dim(subDyad)[1])
				errorChunk	<- cbind(subDyad, errorCode)
				errorFile	<- rbind(errorFile, errorChunk)
			}
			if(dim(app1)[1] < dim(withG1)[1]){ #Fewer approaches than withdraws
				print('more withdraws than approach')
				subDyad	<- rbind(app1, withG1)
	#			print(dim(subDyad))
				subDyad	<- subDyad[order(subDyad$behavior_time),]
	#			print('subDyad reorderd')
				errorCode	<- data.frame(errorCode = rep(2, length = dim(subDyad)[1]))
	#			print(errorCode)
				errorChunk	<- cbind(subDyad, errorCode)
				errorFile	<- rbind(errorFile, errorChunk)
				print('finished adding to error more with than app')
			}
	#		print('exited case')
		}
		if(dim(appcnt)[1] == 0 & dim(with1)[1] != 0){ #There were withdraw 1ms but no contacts, missing contact
			print('There were withdraw 1ms but no contacts, missing contact')
			subDyad	<- rbind(app1, withG1, with1)
			subDyad	<- subDyad[order(subDyad$behavior_time),]
			errorCode	<- rep(3, length = dim(subDyad)[1]) #Need to find approach contacts
			errorChunk	<- cbind(subDyad, errorCode)
			errorFile	<- rbind(errorFile, errorChunk)
		}
		if(dim(appcnt)[1] > 0 & dim(with1)[1] == 0){ #There was a contact, but no withdraw 1m
			print('There was a contact, but no withdraw 1m')
			subDyad	<- rbind(app1, withG1, appcnt)
			subDyad	<- subDyad[order(subDyad$behavior_time),]
			temp		<- data.frame()
			for(p in 2:dim(subDyad)[1]){ #Go through lines consecutively and search for errors
				print(p)
				print(subDyad[p,]$behavior)
				print(subDyad[p-1,]$behavior)
				if(subDyad[p,]$behavior == 'Approach Contact' & subDyad[p-1,]$behavior == 'Approach Contact'){
					print('2 app cnt in a row')
					errorCode	<- rep(4, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Approach 1m' & subDyad[p-1,]$behavior == 'Approach 1m'){
					print('2 app 1m in a row')
					errorCode	<- rep(5, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Approach 1m' & subDyad[p-1,]$behavior == 'Approach Contact'){
					print('app cnt followed by app 1m')
					errorCode	<- rep(6, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Withdraw >1m' & subDyad[p-1,]$behavior == 'Withdraw >1m'){
					print('2 w > 1m in a row')
					errorCode	<- rep(7, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				else{ #if there are no errors at all, can add the appropriate times
					print('no errors adding stop times')
					temp	<- rbind(temp,subDyad[p,])
					if(dim(temp)[1] == dim(subDyad)[1]){ #There were no errors in any line
						for(y in 2:dim(temp)[1]){
							if(temp[y,]$behavior == 'Withdraw >1m'){ #Find the appropriate stop lines
								if(temp[y-1,]$behavior == 'Approach 1m'){ #app 1m/W>1m
									cleanedLines	<- temp[c(y-1, y),]
									startTimes		<- temp[c(y-1,y),]$behavior_time
									stopTimes		<- rep(temp[y,]$behavior_time, 2) ##Fix these lines
									#durations		<- as.numeric(stopTime - startTime)*24*60*60
									cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
									cleanFile		<- rbind(cleanFile, cleanedLines)
								}
								if(temp[(y-1),]$behavior == 'Approach Contact'){
									if(y > 2 & temp[y-2,]$behavior == 'Approach 1m'){ #app 1/app cnt/w>1
										cleanedLines	<- temp[c(y-2:y),]
										startTimes		<- temp[c(y-2:y),]$behavior_time
										stopTimes		<- rep(temp[y,]$behavior_time, 3) ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(y == 2){#app cnt/w>1m at beginning of focal
										cleanedLines	<- rbind(temp[y-1,], temp[y,])
										startTimes		<- temp[c(y-1:y),]$behavior_time
										stopTimes		<- rep(temp[y,]$behavior_time, 2) ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(y > 2 & subDyad[y-2,]$behavior != 'Approach 1m'){ #app cnt/w>1m
										cleanedLines	<- rbind(temp[y-1,], temp[y,])
										startTimes		<- temp[c(y-1:y),]$behavior_time
										stopTimes		<- rep(temp[y,]$behavior_time, 2) ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
								}
							}
							if(y == dim(temp)[1] & temp$behavior != 'Withdraw >1m'){#there are unstopped things in the focal
								if(temp[y,]$behavior == 'Approach 1m'){
									stopTime	<- temp[y,]$focal_end_time #Need to fix this line
									startTime	<- temp[y,]$behavior_time
									#duration	<- as.numeric(stopTime - startTime)
									cleanedLine	<- cbind(temp[y,], startTime, stopTime)
									cleanFile	<- rbind(cleanFile, cleanedLine)
								}
								if(temp[y,]$behavior == 'Approach Contact'){
									if(temp[y-1,]$behavior == 'Approach 1m'){
										cleanedLines	<- rbind(temp[y-1,], temp[y,])
										startTimes		<- temp[c(y-1:y),]$behavior_time
										stopTimes		<- rep(temp[y,]$focal_end_time, 2)
										#durations		<- as.numeric(stopTimes - startTimes
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix this line
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(temp[y-1,]$behavior == 'Withdraw >1m'){
										stopTime	<- temp[y,]$focal_end_time #Need to fix this line
										startTime	<- temp[y,]$behavior_time
										#duration	<- as.numeric(stopTime - startTime)
										cleanedLine	<- cbind(temp[y,], startTime, stopTime) #fix this
										cleanFile	<- rbind(cleanFile, cleanedLine)
									}
									if(temp[y-1,]$behavior == 'Withdraw 1m'){
										byHand	<- temp[lastWithdraw + 1:y, ]
									}
								}
								if(temp[y,]$behavior == 'Withdraw 1m'){
									byHand	<- temp[lastWithdraw + 1:y, ]
								}
							}
						}
					}
				} #close else
			}#clos error checking
		}#close case with 	
		if(dim(appcnt)[1] > 0 & dim(with1)[1] > 0){ #There was a contact, and withdraw 1m
			print('there was a contact and a withdraw 1m')
			subDyad	<- rbind(app1, withG1, appcnt, with1)
			subDyad	<- subDyad[order(subDyad$behavior_time),]
			temp		<- data.frame()
			for(p in 2:dim(subDyad)[1]){ #Go through lines consecutively and search for errors
				print(p)
				print(subDyad[p,]$behavior)
				print(subDyad[p-1,]$behavior)
				if(subDyad[p,]$behavior == 'Approach Contact' & subDyad[p-1,]$behavior == 'Approach Contact'){
					#2 app cnt in a row
					errorCode	<- rep(4, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Approach 1m' & subDyad[p-1,]$behavior == 'Approach 1m'){
					#2 app 1m in a row
					errorCode	<- rep(5, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Approach 1m' & subDyad[p-1,]$behavior == 'Approach Contact'){
					#app cnt followed by app 1m
					errorCode	<- rep(6, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Withdraw >1m' & subDyad[p-1,]$behavior == 'Withdraw >1m'){
					#2 w > 1m in a row
					errorCode	<- rep(7, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Withdraw 1m' & subDyad[p-1,]$behavior == 'Withdraw >1m'){
					#With > 1m followed by withdraw 1m
					errorCode	<- rep(8, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Withdraw 1m' & subDyad[p-1,]$behavior == 'Approach 1m'){
					#App 1m followed by with 1
					errorCode	<- rep(9, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Approach 1m' & subDyad[p-1,]$behavior == 'Withdraw 1m'){
					#Withdraw 1m followed by approach 1m
					errorCode	<- rep(10, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				if(subDyad[p,]$behavior == 'Withdraw 1m' & subDyad[p-1,]$behavior == 'Withdraw 1m'){
					#2 withdraw 1m in a row
					errorCode	<- rep(11, length = dim(subDyad)[1])
					errorChunk	<- cbind(subDyad, errorCode)
					errorFile	<- rbind(errorFile, errorChunk)
				}
				else{ #if there are no errors at all, can add the appropriate times
					print('no errors')
					temp	<- rbind(temp,subDyad[p,])
					if(dim(temp)[1] == dim(subDyad)[1]){ #There were no errors in any line
						###########################################################
						for(y in 2:dim(temp)[1]){ #######Need to add more cases here  - THIS IS THE LAST THING TO DO OTHER THAN JUSE THE CHRON PACKAGE AND DURATIONS
						############################################################
							if(temp[y,]$behavior == 'Withdraw >1m'){ #Find the appropriate stop lines
								lastWithdraw	<- k
								if(temp[y-1,]$behavior == 'Approach 1m'){ #app 1m/W>1m
									cleanedLines	<- temp[c(y-1, y),]
									startTimes		<- temp[c(y-1,y),]$behavior_time
									stopTimes		<- rep(temp[y,]$behavior_time, 2) ##Fix these lines
									#durations		<- as.numeric(stopTime - startTime)*24*60*60
									cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes, durations) #Fix
									cleanFile		<- rbind(cleanFile, cleanedLines)
								}
								if(temp[y-1,]$behavior == 'Approach Contact'){
									if(y > 2 & temp[y-2,]$behavior == 'Approach 1m'){ #app 1/app cnt/w>1
										cleanedLines	<- temp[y-2:y,]
										startTimes		<- temp[y-2:y,]$behavior_time
										stopTimes		<- c(temp[y-1,]$behavior_time, rep(temp[(y),]$behavior_time,2))
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(y > 2 & temp[y-2,]$behavior == 'Withdraw 1m'){ #with1m/app cnt/w>1
										byHand	<- temp[lastWithdraw + 1:y, ]
									}
									if(y == 2){#app cnt/w>1m at beginning of focal
										cleanedLines	<- rbind(temp[y-1,], temp[y,])
										startTimes		<- temp[c(y-1:y),]$behavior_time
										stopTimes		<- rep(temp[y,]$behavior_time, 2) ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(y > 2 & subDyad[y-2,]$behavior != 'Approach 1m'){ #app cnt/w>1m
										cleanedLines	<- rbind(temp[y-1,], temp[y,])
										startTimes		<- temp[c(y-1:y),]$behavior_time
										stopTimes		<- rep(temp[y,]$behavior_time, 2) ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
								}
								if(temp[y-1,]$behavior == 'Withdraw 1m'){ #with1m / withdraw >1m
									if(y == 3){ #app cnt/with 1/ w>1m
										cleanedLines	<- temp[c(y-2, y-1, y),]
										stopTimes		<- c(temp[y-1,]$behavior_time, rep(temp[y,]$behavior_time, 2))
										startTimes		<- temp[c(y-2, y-1, y),]$behavior_time ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(y > 3 & subDyad[y-3,]$behavior == 'Withdraw 1m'){ #....with1/app cnt/with 1/ w>1m
										byHand	<- temp[lastWithdraw + 1:y, ]
									}
									if(y > 3 & subDyad[y-3,]$behavior == 'Approach 1m'){ #app 1/app cnt/with 1/ w>1m
										cleanedLines	<- temp[c(y-3,y-2, y-1, y),]
										stopTimes		<- c(temp[y-2,]$behavior_time, temp[y-1,]$behavior_time, rep(temp[y,]$behavior_time, 2))
										startTimes		<- temp[c(y-3, y-2, y-1, y),]$behavior_time ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(y > 3 & subDyad[y-3,]$behavior == 'Withdraw >1m'){ #/app cnt/with 1/ w>1m
										cleanedLines	<- temp[c(y-2, y-1, y),]
										stopTimes		<- c(temp[y-1,]$behavior_time, rep(temp[y,]$behavior_time, 2))
										startTimes		<- temp[c(y-2, y-1, y),]$behavior_time ##Fix these lines
										#durations		<- as.numeric(stopTime - startTime)*24*60*60
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes) #Fix
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
								}
							}
							if(y == dim(temp)[1] & temp$behavior != 'Withdraw >1m'){#there are unstopped things in the focal
								if(temp[y,]$behavior == 'Approach 1m'){
									stopTime	<- temp[y,]$focal_end_time #Need to fix this line
									startTime	<- temp[y,]$behavior_time
									#duration	<- as.numeric(stopTime - startTime)
									cleanedLine	<- cbind(temp[y,], startTime, stopTime)
									cleanFile	<- rbind(cleanFile, cleanedLine)
								}
								if(temp[y,]$behavior == 'Approach Contact'){
									if(temp[y-1,]$behavior == 'Approach 1m'){
										cleanedLines	<- rbind(temp[y-1,], temp[y,])
										startTimes		<- temp[c(y-1:y),]$behavior_time
										stopTimes		<- rep(temp[y,]$focal_end_time, 2)
										#durations		<- as.numeric(stopTimes - startTimes
										cleanedLines	<- cbind(cleanedLines, startTimes, stopTimes)
										cleanFile		<- rbind(cleanFile, cleanedLines)
									}
									if(temp[y-1,]$behavior == 'Withdraw >1m'){
										stopTime	<- temp[y,]$focal_end_time #Need to fix this line
										startTime	<- temp[y,]$behavior_time
										#duration	<- as.numeric(stopTime - startTime)
										cleanedLine	<- cbind(temp[y,], startTime, stopTime)
										cleanFile	<- rbind(cleanFile, cleanedLine)
									}
									if(temp[y-1,]$behavior == 'Withdraw 1m'){
										byHand	<- temp[lastWithdraw + 1:y, ]
									}
								}
								if(temp[y,]$behavior == 'Withdraw 1m'){
									byHand	<- temp[lastWithdraw + 1:y, ]
								}
							}
						}
					}
				} #close else
			} #Close for loop
		} #close case
	print(paste('finished cleaning', d))
	} #close dyad for loop
	#print('exited for loop')
	#print(dim(cleanFile))
	#print(dim(errorFile))
	#print(dim(byHand))
	#print(byHand)
	#Need to add obs #, duraiton column, fixfocal start time
	return(list(cleanFile, errorFile, byHand))
}

listFocals	<- unique(focalDataID$focal_start_chron)
cleanedTest	<- cleanData(test, cleanedData, errorFile)

cleanAllFocalData	<- function(data, cleanFile, errorFile, byHand){
	### This function takes in an entire database, seperates it out by focal, cleans each focal individually, and returns the clean and error files
	### Data is a dataset from AO (can contain multiple focals of data)
	### cleanFile is a blank data.frame
	### errorFile is a blank data.frame
	listFocals	<- unique(data$focal_start_time)
	for(i in 1:length(listFocals)){
		#print(paste('Beginning to clean', listFocals[i]))
		focalData	<- seperateFocals(data, listFocals[i])
		#print(paste(dim(focalData)[1], 'lines of data'))
		if(i == 1){
			output		<- cleanData(focalData, cleanFile, errorFile, byHand)
		}
		if(i > 1){
			output		<- cleanData(focalData, output[[1]], output[[2]], output[[3]])
		}
		print(paste(listFocals[i], 'has been cleaned'))
	}
	colnames(output[[1]])
	cleanFileSimple <- output[[1]][,c('pin_code_name', 'focal_start_time', 'focal_individual_id','observation_number', 'startTime', 'stopTime',
			'actor', 'subject', 'context', 'food_item', 'part_eaten', 'behavior','tree_species', 'tree_number',
			'response', 'response_behavior', 'winner', 'comment')]
	return(list(cleanFileSimple, output[[2]], output[[3]]))
}

cleanFocal	<- data.frame()
errorFocal	<- data.frame()
byHand	<- data.frame()

rawDataNoError	<- focalDataNoNA[which(focalDataNoNA$focal_start_time >= '2019-02-24 11:30:56'),]
errorRows		<- focalDataNoNA[which(focalDataNoNA$focal_start_time == '2019-02-24 10:30:33'),]
clean1	<- cleanAllFocalData(rawDataNoError, cleanFocal, errorFocal, byHand)

dbDisconnect(con)
dbUnloadDriver(drv)

