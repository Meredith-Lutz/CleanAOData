##################################
##### Compile data for Brynn #####
##################################
setwd('G:/My Drive/Graduate School/Research/Projects/Brynn')

library(RPostgreSQL)
library(sp)
library(rgdal)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "verreauxi_2019",
                 host = "localhost", port = 5432,
                 user = "postgres", password = "postgres")

scanData	<- dbGetQuery(con, "SELECT * from main_tables.all_scan_data_view_2")

scanDataSimp	<- scanData[scanData$focal_individual_id == scanData$scanned_individual_id,c('group_id', 'focal_start_time', 'scan_time', 'scanned_individual_id',
				'activity', 'height', 'x_position', 'y_position', 'latitude', 'longitude', 'compass_bearing',
				'quadrat', 'distance_to_group')]

scanDataSort	<- scanDataSimp[order(scanDataSimp$scan_time, scanDataSimp$distance_to_group),]

scanDataNoDup	<- scanDataSort[!duplicated(scanDataSort[,1:12]),]
scanDataNoDup	<- scanDataNoDup[1:1328,]

restCats	<- c('Rest')
ffCats	<- c('Feed', 'Forage')
movCats	<- c('Travel')
otherCats	<- c('Autogroom', 'Mating', 'Play', 'Scan', 'Scent mark', 'Social')

for(i in 1:dim(scanDataNoDup)[1]){
	behavior	<- scanDataNoDup[i,c('activity')]
	#print(i)
	#print(behavior)
	if(is.na(scanDataNoDup[i,]$latitude == TRUE)){
		scanDataNoDup[i,]$latitude	<- 0
		scanDataNoDup[i,]$longitude	<- 0
		scanDataNoDup[i,]$compass_bearing	<- 0
	}
	if(behavior == 'Rest'){
		scanDataNoDup[i,c('behavAbb')]	<- 'R'
	}
	else if(behavior == 'Feed' | behavior == 'Forage'){
		scanDataNoDup[i,c('behavAbb')]	<- 'F'
	}
	else if(behavior == 'Travel'){
		scanDataNoDup[i,c('behavAbb')]	<- 'T'
	}
	else{
		scanDataNoDup[i,c('behavAbb')]	<- 'O'
	}
}



deg.to.utm <- function(dd, crs = "+proj=utm +zone=38 +south +datum=WGS84")
{
	temp <- SpatialPoints(dd, proj4string=CRS("+proj=longlat +datum=WGS84"))
	data.frame(spTransform(temp, CRS(crs)))
}

utm	<- deg.to.utm(scanDataNoDup[,c('longitude', 'latitude')])

scanDataUTM	<- cbind(scanDataNoDup, utm)
colnames(scanDataUTM)[15]	<- 'utmLon'
colnames(scanDataUTM)[16]	<- 'utmLat'

calculateAnimalXY	<- function(lat, lon, theta, d){
	sifakaXY	<- data.frame(sifakaX = numeric(), sifakaY = numeric())
	#print(sifakaXY)
	for(i in 1:length(lat)){
		#print(i)
		#print(lon[i])
		#print(theta[i])
		#print(d[i])
		if(is.na(theta[i] == TRUE)){
			xAnim		<- NA
			yAnim		<- NA
		}
		else if(theta[i] < 90 & theta[i] >= 0){
			rad		<- theta[i] * pi/180
			dx		<- d[i] * sin(rad)
			dy		<- d[i] * cos(rad)
			xAnim		<- lon[i] + dx
			yAnim		<- lat[i] + dy
		}
		else if(theta[i] < 180 & theta[i] >= 90){
			theta2	<- theta[i] - 90
			rad		<- theta2 * pi/180
			dx		<- d[i] * cos(rad)
			dy		<- d[i] * sin(rad)
			xAnim		<- lon[i] + dx
			yAnim		<- lat[i] - dy
		}
		else if(theta[i] < 270 & theta[i] >= 180){
			theta2	<- theta[i] - 180
			rad		<- theta2 * pi/180
			dx		<- d[i] * cos(rad)
			dy		<- d[i] * sin(rad)
			xAnim		<- lon[i] - dx
			yAnim		<- lat[i] - dy
		}
		else if(theta[i] < 360 & theta[i] >= 270){
			theta2	<- 360 - theta[i]
			rad		<- theta2 * pi/180
			dx		<- d[i] * sin(rad)
			dy		<- d[i] * cos(rad)
			xAnim		<- lon[i] - dx
			yAnim		<- lat[i] + dy
		}
		sifakaXY[i, 1]	<- xAnim
		sifakaXY[i, 2]	<- yAnim
	}
	return(sifakaXY)
}

sifakaXY	<- calculateAnimalXY(scanDataUTM$utmLat, scanDataUTM$utmLon, scanDataUTM$compass_bearing, as.numeric(scanDataUTM$distance_to_group))
scanDataUTMAnimal	<- cbind(scanDataUTM, sifakaXY)

write.csv(scanDataUTMAnimal, 'sifakaRainySeasonMeredith.csv')