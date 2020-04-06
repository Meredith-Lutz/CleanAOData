##Code to change AO scan format to Becca's scan format
library(chron)
library(RPostgreSQL)
library(lubridate)
 
# loads the PostgreSQL driver
calculateDistance	<- function(partnerX, partnerY, partnerZ, focalZ){
	distance	<- sqrt(partnerX^2 + partnerY^2 + (focalZ - partnerZ)^2)
	return(distance)
}

distanceCategory	<- function(distance){
	if(distance <= .05){
		distanceCat <-0
	}
	if(distance > .05 & distance <= 1){
		distanceCat	<- 1
	}
	if(distance > 1 & distance <= 5){
		distanceCat	<- 5
	}
	if(distance > 5 & distance <= 10){
		distanceCat	<- 10
	}
	if(distance > 10){
		distanceCat <- '+'
	}
	return(distanceCat)
}

#checkSocialData	<- function(scanTime, socialData){
#	startTime	<- scanTime - dminutes(10)
	#print(startTime)
	endTime	<- scanTime
	#print(endTime)
	timeInt	<- as.interval(startTime, endTime)
	print(interval)
	prevData	<- socialData[socialData$behavior_time %within% timeInt,]
	#print(dim(prevData))
	print(head(prevData))
	if(dim(prevData)[1] > 0){ #There is social data
		return(list(1, 0))
	}
	if(dim(prevData)[1] == 0){ #No social data
		return(list(0, 1))
	}
}

identifyNN		<- function(scanLine){
	cols		<- c(23, 34, 45, 56, 67, 78)
	distances	<- scanLine[cols]
	minPos	<- which(distances == min(distances, na.rm = TRUE))
	distances2	<- distances[distances > min(distances, na.rm = TRUE)]
	minPos2	<- which(distances == min(distances2, na.rm = TRUE))
	NN		<- scanLine[cols[minPos]-9][1,1]
	NN2		<- scanLine[cols[minPos2]-9][1,1]
	return(list(NN, NN2))
}
	
cleanScanData	<- function(aoFormat, scanVars, focalVars, socialData){
	#Extract list of scans
	aoFormat	<- aoFormat[is.na(aoFormat$focal_individual_id) == FALSE,]
	aoFormat$scan_time	<- ymd_hms(aoFormat$scan_time)
	scans		<- unique(aoFormat$scan_time)
	colNamesRep	<- c('indiv', 'activity', 'part', 'quadrat', 'treeNum', 'tree_species',
		 'x', 'y', 'height', 'distance', 'distanceCat')

	#Create empty dataframe
	toFill		<- data.frame(matrix(ncol = 78))
	colnames(toFill)	<- c('time', 'focal', 'focalActivity', 'focalPart', 'focalGrid',
	 	'focalTreeNum', 'focalTreeSpecies', 'focalX', 'focalY', 'focalHeight',
		'focalDistance', 'focalDistanceCat', rep(colNamesRep, 6))

	#Fill dataframe with scan data
	for(i in 1:length(scans)){
		data			<- aoFormat[aoFormat$scan_time == scans[i],]
		data			<- data[order(abs(data$x_position)),]
		#print(data)
		nIndiv		<- dim(data)[1]
		#print(nIndiv)
		toFill[i, 1]	<- as.character(scans[i])
		print(paste('Cleaning', as.character(scans[i])))
		for(j in 1:nIndiv){
			toFill[i,((j-1)*11)+2]	<- data[j,5] #ID
			toFill[i,((j-1)*11)+3]	<- data[j,6] #Behav
			toFill[i,((j-1)*11)+4]	<- data[j,7] #Part
			toFill[i,((j-1)*11)+5]	<- data[j,8] #quadrat
			toFill[i,((j-1)*11)+6]	<- data[j,9] #tree_num
			toFill[i,((j-1)*11)+7]	<- data[j,10] #tree_species
			toFill[i,((j-1)*11)+8]	<- data[j,11] #x
			toFill[i,((j-1)*11)+9]	<- data[j,12] #y
			toFill[i,((j-1)*11)+10]	<- data[j,13] #height
			toFill[i,((j-1)*11)+11]	<- calculateDistance(as.numeric(toFill[i,((j-1)*11)+8]), as.numeric(toFill[i,((j-1)*11)+9]), as.numeric(toFill[i,((j-1)*11)+10]), as.numeric(toFill[i,10])) 
			#print(toFill[i,((j-1)*11)+11])
			toFill[i,((j-1)*11)+12]	<- distanceCategory(as.numeric(toFill[i,((j-1)*11)+11]))
		}
	}

	#Create datasets for each scan variable
	number	<- scanVars[scanVars$scanvars == 'Number of Individuals', c('scan_time', 'number_of_individuals')] 
	change	<- scanVars[scanVars$scanvars == 'Change in membership?', c('scan_time', 'change_in_membership')] 
	groupSpread	<- scanVars[scanVars$scanvars == 'Group spread', c('scan_time', 'group_spread')]
	groupSpread$scan_time	<- ymd_hms(groupSpread$scan_time)
	change$scan_time		<- ymd_hms(change$scan_time)
	number$scan_time		<- ymd_hms(number$scan_time)
	
	#Merge scan variables onto scan data
	toFill$date		<- as.Date(toFill$time)
	toFill$time		<- ymd_hms(toFill$time)
	toFill2		<- merge(toFill, groupSpread, by.x = 'time', by.y = 'scan_time')
	toFill3		<- merge(toFill2, change, by.x = 'time', by.y = 'scan_time')
	toFill4		<- merge(toFill3, number, by.x = 'time', by.y = 'scan_time')

	#Merge focal #'s onto dataset
	extras		<- aoFormat[!duplicated(aoFormat$scan_time),c('scan_time', 'pin_code_name', 'focal_start_time', 'observation_number')]
	extras$scan_time		<- ymd_hms(extras$scan_time)
	toFill5	<- merge(toFill4, extras, by.x = 'time', by.y = 'scan_time')
	
	#Create datasets for each focal variables
	sky		<- focalVars[focalVars$focalvars == 'Sky', c('focal_start_time', 'sky')]
	temperature	<- focalVars[focalVars$focalvars == 'Temperature', c('focal_start_time', 'temperature')]
	
	#Merge focal variables onto dataset
	toFill6	<- merge(toFill5, sky, by.x = 'focal_start_time', by.y = 'focal_start_time')
	toFill7	<- merge(toFill6, temperature, by.x = 'focal_start_time', by.y = 'focal_start_time')
	
	#Add socialdata and NN
	toFill7$socialData	<- NA
	toFill7$noSocialData	<- NA
	toFill7$NNID	<- NA
	toFill7$NNID2	<- NA
	for(j in 1:dim(toFill7)[1]){
		line	<- toFill7[j,]
		toFill7[j,]$socialData	<- checkSocialData(line$time, socialData)[[1]]
		toFill7[j,]$noSocialData	<- checkSocialData(line$time, socialData)[[2]]
		toFill7[j,]$NNID	<- identifyNN(line)[[1]]
		toFill7[j,]$NNID2	<- identifyNN(line)[[2]]
	}

	#Reorder and rename columns
	finalData			<- toFill7[,c(84, 80, 3, 85, 86:87, 81, 2, 4:79, 90, 89, 88, 91, 83, 82)]

	return(finalData)
}

setwd('D:/Google Drive/Graduate School/Research/FieldSeason2019KMNP')
drv	<- dbDriver("PostgreSQL")

#Establishes connection
con	<- dbConnect(drv, dbname = "verreauxi_2019_v1_3",
		host = "localhost", port = 5432,
		user = "postgres", password = 'Animalbehavior1#')

scanVariables <- dbGetQuery(con, "SELECT * from main_tables.scan_variables")
focalVariables <- dbGetQuery(con, "SELECT * from main_tables.focal_variables")

scanData <- dbGetQuery(con, "SELECT * from main_tables.all_scan_data_view")
socialData	<- dbGetQuery(con, "SELECT * from main_tables.all_focal_data_view")

simpleScan	<- scanData[,c('pin_code_name', 'focal_start_time', 'focal_individual_id',
	'scan_time', 'scanned_individual_id', 'activity', 'part_eaten', 'quadrat',
	'tree_number', 'tree_species', 'x_position', 'y_position', 'height', 'observation_number')]
simpleScanNoNA	<- simpleScan[is.na(simpleScan$x_position) == FALSE,]
socialDataNoNA	<- socialData[is.na(socialData$longitude) == FALSE,]

cleaned1_3	<- cleanScanData(simpleScanNoNA, scanVariables, focalVariables, socialDataNoNA)
write.csv(cleaned1_3, 'scans1_3.csv')
	