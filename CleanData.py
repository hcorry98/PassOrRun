import glob

import pandas as pd

class CleanData:

    DIRTY_PATH = 'data/dirty/nfl-big-data-bowl-20'
    CLEAN_PATH = 'data/clean/20'

    def __init__(self):
        self.ballDict = {}

    def findFiles(self):
        self.files = {}
        self.files[23] = glob.glob(f'{self.DIRTY_PATH}23/**/*.csv', recursive=True)
        self.files[24] = glob.glob(f'{self.DIRTY_PATH}24/**/*.csv', recursive=True)

    def _buildBallDict(self, df):
        self.ballDict = {}
        ballDf = df.copy()

        ballDf = ballDf.loc[ballDf['team'] == 'football']
        ballDf['uniqueId'] = ballDf['gameId'].astype(str) + '_' + ballDf['playId'].astype(str)

        ballDf.apply(self.__buildDict, axis=1)

    def __buildDict(self, row):
        self.ballDict[row['uniqueId']] = (row['x'], row['y'])

    def __centerX(self, row):
        return row['x'] - self.ballDict[row['uniqueId']][0]

    def __centerY(self, row):
        return row['y'] - self.ballDict[row['uniqueId']][1]

    def _centerPlayers(self, df):
        df['uniqueId'] = df['gameId'].astype(str) + '_' + df['playId'].astype(str)
        df['x'] = df.apply(self.__centerX, axis=1)
        df['y'] = df.apply(self.__centerY, axis=1)
        return df

    def _cleanTracking(self, file):
        df = pd.read_csv(file)
        snapEvents = ['ball_snap', 'autoevent_ballsnap']
        df = df.loc[df['event'].isin(snapEvents)]
        df = df.drop_duplicates(subset=['gameId', 'playId', 'nflId'], keep='first')

        colsToDrop = ['frameId', 'time', 'jerseyNumber', 'event']
        if 'displayName' in df.columns:
            colsToDrop.append('displayName')
        df = df.drop(columns=colsToDrop)

        if 'club' in df.columns:
            df = df.rename(columns={'club': 'team'})

        self._buildBallDict(df)
        df = self._centerPlayers(df)

        return df

    def _cleanPlayers(self, year):
        players = pd.read_csv(f'{self.DIRTY_PATH}{year}/players.csv')
        players = players.drop(columns=['height', 'weight', 'birthDate', 'collegeName'])
        if 'officialPosition' in players.columns:
            players = players.rename(columns={'officialPosition': 'position'})
        return players


    def _cleanPlays(self, year):
        plays = pd.read_csv(f'{self.DIRTY_PATH}{year}/plays.csv')
        plays = plays[[
            'gameId', 'playId', 'quarter', 'down', 'yardsToGo', 'possessionTeam', 'gameClock',
            'preSnapHomeScore', 'preSnapVisitorScore', 'absoluteYardlineNumber',
            'offenseFormation', 'passResult'
        ]]

        plays['year'] = 2000 + year

        plays['playType'] = 'pass'
        plays.loc[plays['passResult'].isnull(), 'playType'] = 'run'
        plays = plays.drop(columns=['passResult'])

        return plays

    def __calcWinningBy(self, row):
        if row['homeTeamAbbr'] == row['team']:
            return row['preSnapHomeScore'] - row['preSnapVisitorScore']
        else:
            return row['preSnapVisitorScore'] - row['preSnapHomeScore']

    def _getWinningBy(self, master, year):
        games = pd.read_csv(f'{self.DIRTY_PATH}{year}/games.csv')
        games = games[['gameId', 'homeTeamAbbr', 'visitorTeamAbbr']]

        master = pd.merge(master, games, how='left', on='gameId')
        master['winningBy'] = master.apply(self.__calcWinningBy, axis=1)

        master = master.drop(columns=['homeTeamAbbr', 'visitorTeamAbbr', 'preSnapHomeScore', 'preSnapVisitorScore'])
        return master

    def clean(self):
        for year, files in self.files.items():
            print(f'Cleaning 20{year}.')
            print(f'Reading data...') 
            master = pd.DataFrame()
            for file in files:
                if file[-5].isdigit():
                    df = self._cleanTracking(file)
                    master = pd.concat([master, df])

            players = self._cleanPlayers(year)
            plays = self._cleanPlays(year)

            master = pd.merge(master, players, how='left', on='nflId')
            master = pd.merge(master, plays, how='left', on=['gameId', 'playId'])

            master['offense'] = False
            master.loc[master['team'] == master['possessionTeam'], 'offense'] = True
            master = master.drop(columns=['possessionTeam'])

            master = self._getWinningBy(master, year)

            master = master[[
                'gameId', 'playId', 'offense', 'position', 'displayName', 'team',
                'x', 'y', 's', 'a', 'dis', 'o', 'dir', 'offenseFormation', 'playDirection',
                'down', 'yardsToGo', 'absoluteYardlineNumber', 'quarter', 'gameClock',
                'winningBy', 'year', 'playType'
            ]]

            print('Saving cleaned data...')
            master = master.drop_duplicates()
            master = master.dropna()
            master.to_csv(f'{self.CLEAN_PATH}{year}.csv', index=False)
            print()

        print('Finished cleaning data.')
        print()

    def combine(self):
        print('Combining cleaned data...')
        df23 = pd.read_csv(f'{self.CLEAN_PATH}23.csv')
        df24 = pd.read_csv(f'{self.CLEAN_PATH}24.csv')

        master = pd.concat([df23, df24])
        master.to_csv(f'{self.CLEAN_PATH[:-2]}combined.csv', index=False)
        print('Finished combining cleaned data.')



if __name__ == '__main__':
    cd = CleanData()
    cd.findFiles()
    cd.clean()
    cd.combine()
