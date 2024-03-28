import glob

import pandas as pd

class CleanData:

    DIRTY_PATH = 'data/dirty/nfl-big-data-bowl-20'
    CLEAN_PATH = 'data/clean/20'

    def findFiles(self):
        self.files = {}
        self.files[23] = glob.glob(f'{self.DIRTY_PATH}23/**/*.csv', recursive=True)
        self.files[24] = glob.glob(f'{self.DIRTY_PATH}24/**/*.csv', recursive=True)

    def __cleanPlays(self, year):
        plays = pd.read_csv(f'{self.DIRTY_PATH}{year}/plays.csv')
        plays = plays[[
            'gameId', 'playId', 'quarter', 'down', 'yardsToGo', 'possessionTeam', 'gameClock',
            'preSnapHomeScore', 'preSnapVisitorScore', 'absoluteYardlineNumber',
            'offenseFormation', 'passResult'
        ]]

        # TODO: change preSnapScores to point differential

        plays['year'] = 2000 + year

        plays['playType'] = 'pass'
        plays.loc[plays['passResult'].isnull(), 'playType'] = 'run'
        plays = plays.drop(columns=['passResult'])

        return plays

    def __cleanPlayers(self, year):
        players = pd.read_csv(f'{self.DIRTY_PATH}{year}/players.csv')
        players = players.drop(columns=['height', 'weight', 'birthDate', 'collegeName'])
        if 'officialPosition' in players.columns:
            players = players.rename(columns={'officialPosition': 'position'})
        return players

    def __cleanTracking(self, file):
        df = pd.read_csv(file)
        df = df.loc[df['event'] == 'ball_snap']

        colsToDrop = ['frameId', 'time', 'jerseyNumber', 'event']
        if 'displayName' in df.columns:
            colsToDrop.append('displayName')
        df = df.drop(columns=colsToDrop)

        if 'club' in df.columns:
            df = df.rename(columns={'club': 'team'})

        # TODO: center coordinates on ball
        
        return df

    def clean(self):
        for year, files in self.files.items():
            print(f'Cleaning 20{year}.')
            print(f'Reading data...') 
            master = pd.DataFrame()
            for file in files:
                if file[-5].isdigit():
                    df = self.__cleanTracking(file)
                    master = pd.concat([master, df])

            players = self.__cleanPlayers(year)
            plays = self.__cleanPlays(year)

            master = pd.merge(master, players, how='inner', on='nflId')
            master = pd.merge(master, plays, how='inner', on=['gameId', 'playId'])

            master['offense'] = False
            master.loc[master['team'] == master['possessionTeam'], 'offense'] = True
            master = master.drop(columns=['possessionTeam'])

            master = master[[
                'gameId', 'playId', 'offense', 'position', 'displayName', 'team',
                'x', 'y', 's', 'a', 'dis', 'o', 'dir', 'offenseFormation', 'playDirection',
                'down', 'yardsToGo', 'absoluteYardlineNumber', 'quarter', 'gameClock',
                'preSnapHomeScore', 'preSnapVisitorScore', 'year', 'playType'
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
