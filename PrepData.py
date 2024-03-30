import pandas as pd

class PrepData:
    def __init__(self):
        self.masterDf = pd.read_csv('data/clean/combined.csv')

    def _removeDefense(self, df):
        df = df[df['offense'] == True]
        df = df.drop(columns=['offense'])
        return df

    def _getPlays(self, df):
        plays = df.drop_duplicates(subset=['gameId', 'playId'])
        plays['uniqueId'] = plays['gameId'].astype(str) + '_' + plays['playId'].astype(str)
        plays = plays[[
            'uniqueId', 'gameId', 'playId', 'team', 'offenseFormation', 'playDirection',
            'down', 'yardsToGo', 'absoluteYardlineNumber', 'quarter', 'gameClock',
            'winningBy', 'year', 'playType'
        ]]
        return plays
    
    def _combinePlayerLocsPerPlay(self, df, plays):
        pass

    def prep(self):
        print('Prepping data...')
        self.masterDf = self._removeDefense(self.masterDf)
        self.masterDf.to_csv('data/prep/master.csv', index=False)
        plays = self._getPlays(self.masterDf)
        plays.to_csv('data/prep/plays.csv', index=False)
        print('Data prepped.')



if __name__ == '__main__':
    pd = PrepData()
    pd.prep()