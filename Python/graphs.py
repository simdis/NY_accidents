import pandas as pd
import numpy as np
import matplotlib.pyplot as mpl
import copy
import time

# Plot define
mpl.show(block=True)
# Absolute path
path_to_dataset = ''
csv_name = 'NYPD_Motor_Vehicle_Collisions'

def compute_total_weeks(dataframe):
	#Print minimum and maximum timestamps available
	min_timestamp = min(dataframe['Datetime'])
	max_timestamp = max(dataframe['Datetime'])
	print('Minimum timestamp is {}'.format(min_timestamp))
	print('Maximum timestamp is {}'.format(max_timestamp))
	min_week = min_timestamp.week
	min_year = min_timestamp.year
	max_week = max_timestamp.week
	max_year = max_timestamp.year
	# Correct, if any, the year (see below)
	if (min_timestamp.month == 12 and min_week == 1):
		min_year += 1
	elif (min_timestamp.month == 1 and min_week == 52):
		min_year -= 1
	# Correct, if any, the year (see below)
	if (max_timestamp.month == 12 and max_week == 1):
		max_year += 1
	elif (max_timestamp.month == 1 and max_week == 52):
		max_year -= 1
	# Total weeks are those of "complete" years plus those of first year and 
	# those of the last year
	total_weeks = (max_year - min_year - 1)*52 + (52-min_week+1) + (max_week)
	print('There are {} weeks into the dataset'.format(total_weeks))
	return total_weeks

if __name__ == "__main__":
	accidents_ny = pd.read_csv('{}{}.csv'.format(path_to_dataset,csv_name), \
		delimiter=',')

	#print(accidents_ny.head())
	print('The available attributes are:\n' + '\n'.join(accidents_ny.columns.values))

	'''#Convert the ZIP CODE to integer. The Nan are set to -1
	print('Correcting the Zip code...')
	s_t = time.time()
	accidents_ny['ZIP CODE'] = accidents_ny['ZIP CODE'].fillna(-1).astype(int)
	print('Required time... {} s'.format(time.time() - s_t))'''

	#Create a new column Datetime that joins the date and the time ones.
	print('Converting to datetime...')
	s_t = time.time()
	accidents_ny['Datetime'] = accidents_ny['DATE'] + ' ' + accidents_ny['TIME']
	del accidents_ny['DATE']
	del accidents_ny['TIME']

	#Convert to timestamp the data column
	accidents_ny['Datetime'] = pd.to_datetime(accidents_ny['Datetime'],\
		format="%m/%d/%Y %H:%M")
	print('Required time... {} s'.format(time.time() - s_t))

	#print(accidents_ny.head())

	############################################################################
	############################################################################
	############################### FATAL ACCIDENTS ############################
	############################################################################
	############################################################################

	#Create a dataframe containing exactly the lethal accidents
	print('First step... how many lethal accidents per week are there?')
	s_t = time.time()

	accidents_ny_lethal = \
		accidents_ny[['NUMBER OF PERSONS KILLED','Datetime']].copy()
	accidents_ny_lethal = \
		accidents_ny_lethal[accidents_ny_lethal['NUMBER OF PERSONS KILLED']>0]
	#Group by week (and year)
	accidents_ny_lethal.index = accidents_ny_lethal['Datetime']
	times = pd.DatetimeIndex(accidents_ny_lethal['Datetime'])
	# Remove the columns after it is become the index
	del accidents_ny_lethal['Datetime']

	#Define the year correction to deal with particular week numbers 
	#For instance, the first of January may be in the 52th week of previous year)
	times_year_correction = ((times.month/12)*(52-times.week)/51)-\
		((12-times.month)/11*(times.week/52))
	times_year_correction = times_year_correction.astype(np.int32)
	    
	accidents_ny_lethal_g = accidents_ny_lethal.\
		groupby([times.year+times_year_correction,times.week]).agg(['count','sum'])
	accidents_ny_lethal_g.columns=['Fatal Accidents','Deaths']

	print(accidents_ny_lethal_g.head())

	#Plot the graph
	accidents_ny_lethal_g['Fatal Accidents'].plot(figsize=(15,10))
	mpl.savefig('./figures/{}_lethal_accidents_per_week.pdf'.format(csv_name))
	#Save on a csv
	accidents_ny_lethal_g.to_csv('./output/'+\
		'{}_lethal_accidents_per_week.csv'.format(csv_name), sep=',',\
			index_label=['Year','Week'])

	print('Required time... {} s'.format(time.time() - s_t))

	############################################################################
	############################################################################
	############################# CONTRIBUTING FACTORS #########################
	############################################################################
	############################################################################

	print('Second step... how many lethal accidents per contributing factors are there?')
	print('Observation: only the contributing factor of the first vehicle '+\
		'is used, when specified.')
	s_t = time.time()

	accidents_ny_cf = accidents_ny[['CONTRIBUTING FACTOR VEHICLE 1',\
		'NUMBER OF PERSONS KILLED']].copy()
	accidents_ny_cf['CONTRIBUTING FACTOR VEHICLE 1'] = \
		accidents_ny_cf['CONTRIBUTING FACTOR VEHICLE 1'].fillna('Unspecified')
	accidents_ny_cf_g = accidents_ny_cf.groupby('CONTRIBUTING FACTOR VEHICLE 1').\
		agg(['count','sum'])
	accidents_ny_cf_g.columns = ['Total Accidents', 'Total Deaths']
	accidents_ny_cf_g['Average Deaths'] = \
		(accidents_ny_cf_g['Total Deaths'].astype(np.float32))/\
			accidents_ny_cf_g['Total Accidents']

	print(accidents_ny_cf_g.head())

	#Plot the graph (without unspecified field)
	accidents_ny_cf_g = accidents_ny_cf_g.reset_index()
	accidents_ny_cf_g = accidents_ny_cf_g[accidents_ny_cf_g\
		['CONTRIBUTING FACTOR VEHICLE 1'] != 'Unspecified']
	accidents_ny_cf_g.index = accidents_ny_cf_g['CONTRIBUTING FACTOR VEHICLE 1']
	del accidents_ny_cf_g['CONTRIBUTING FACTOR VEHICLE 1']
	accidents_ny_cf_g['Total Accidents'].plot(figsize=(15,10), kind='bar')
	mpl.savefig('./figures/{}_total_accidents_per_contributing_factor.pdf'.\
		format(csv_name))
	# Clear the current figure
	mpl.clf()
	accidents_ny_cf_g['Average Deaths'].plot(figsize=(15,10), kind='bar')
	mpl.savefig('./figures/{}_average_deaths_per_contributing_factor.pdf'.\
		format(csv_name))
	#Save on a csv
	accidents_ny_cf_g.to_csv('./output/'+\
		'{}__total_accidents_per_contributing_factor.csv'.format(csv_name),\
			sep=',',index_label=['Contributing Factor'])


	print('Required time... {} s'.format(time.time() - s_t))

	############################################################################
	############################################################################
	################ WEEKLY AVERAGE NUMBER OF ACCIDENTS PER BOROUGH ############
	############################################################################
	############################################################################

	print('Third step... how many accidents are there per borough weekly in average?')
	s_t = time.time()

	accidents_ny_borough = \
		accidents_ny[['NUMBER OF PERSONS KILLED', 'Datetime', 'BOROUGH']]\
			.copy().fillna('Unknown')
	times = pd.DatetimeIndex(accidents_ny_borough['Datetime'])
	# Convert the column 'NUMBER OF PERSONS KILLED' into 0-1's value column.
	accidents_ny_borough['NUMBER OF PERSONS KILLED'] = \
		accidents_ny_borough['NUMBER OF PERSONS KILLED'].astype(bool)
	accidents_ny_borough['NUMBER OF PERSONS KILLED'] = \
		accidents_ny_borough['NUMBER OF PERSONS KILLED'].astype(np.int32)

	#Define the year correction to deal with particular week numbers 
	#For instance, the first of January may be in the 52th week of previous year)
	times_year_correction = ((times.month/12)*(52-times.week)/51)-\
		((12-times.month)/11*(times.week/52))
	times_year_correction = times_year_correction.astype(np.int32)

	# Group by week and borough
	accidents_ny_borough_g = accidents_ny_borough.groupby(['BOROUGH',times.year+\
		times_year_correction,times.week])['NUMBER OF PERSONS KILLED'].\
			agg(['count','sum'])
	accidents_ny_borough_g.columns = ['Total accidents', 'Lethal accidents']

	print(accidents_ny_borough_g.head())

	#Plot a graph per week
	mpl.clf()
	# Unstack the borough.
	aa = accidents_ny_borough_g.unstack(0)
	aa['Total accidents'].plot(figsize=(1500,100), kind='bar')
	mpl.savefig('./figures/{}_total_accidents_weekly_per_borough.pdf'.\
		format(csv_name))

	#Save on a csv
	accidents_ny_borough_g.to_csv('./output/'+\
		'{}__weekly_accidents_per_borough.csv'.format(csv_name), sep=',',\
			index_label=['Borough', 'Year', 'Week'])

	# Compute the averages per borough by simply summing the lethal accidents and,
	# then, by dividing by the total number of weeks
	print('Computing the total number of weeks...')
	total_weeks = compute_total_weeks(accidents_ny)
	accidents_ny_borough_weekly_g = accidents_ny_borough_g.reset_index()
	accidents_ny_borough_weekly_g = accidents_ny_borough_weekly_g.groupby('BOROUGH')\
		['Lethal accidents'].agg(['sum'])
	accidents_ny_borough_weekly_g.columns = ['Lethal accidents']
	accidents_ny_borough_weekly_g['Weekly lethal accidents'] = \
		accidents_ny_borough_weekly_g['Lethal accidents'] / total_weeks

	print(accidents_ny_borough_weekly_g)

	#Plot a graph
	mpl.clf()
	accidents_ny_borough_weekly_g['Weekly lethal accidents'].plot(figsize=(15,10),\
		kind='bar')
	mpl.savefig('./figures/{}_average_lethal_accidents_weekly_per_borough.pdf'.\
		format(csv_name))

	#Save on a csv
	accidents_ny_borough_weekly_g.to_csv('./output/'+\
		'{}_average_weekly_lethal_accidents_per_borough.csv'.format(csv_name),\
			sep=',',index_label=['Borough'])

	print('Required time... {} s'.format(time.time() - s_t))
