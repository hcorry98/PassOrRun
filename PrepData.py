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
        plays['uniqueId'] = plays['gameId'].astype(str) + '_' + plays['playId'].astype(str).str.zfill(4)
        plays = plays[[
            'uniqueId', 'gameId', 'playId', 'team', 'offenseFormation', 'playDirection',
            'down', 'yardsToGo', 'absoluteYardlineNumber', 'quarter', 'gameClock',
            'winningBy', 'year', 'playType'
        ]]
        return plays
    
    def _combinePlayerLocsPerPlay(self, df):
        uniqueId = df['gameId'].astype(str) + '_' + df['playId'].astype(str).str.zfill(4)
        df.insert(0, 'uniqueId', uniqueId)

        df = df.sort_values(by=['uniqueId', 'position'])

        df = df.groupby('uniqueId').agg({
            'gameId': 'first',
            'playId': 'first',
            'team': 'first',
            'offenseFormation': 'first',
            'playDirection': 'first',
            'down': 'first',
            'yardsToGo': 'first',
            'absoluteYardlineNumber': 'first',
            'quarter': 'first',
            'gameClock': 'first',
            'winningBy': 'first',
            'year': 'first',
            'position': list,
            'displayName': list,
            'x': list,
            'y': list,
            's': list,
            'a': list,
            'dis': list,
            'o': list,
            'dir': list,
            'playType': 'first'
        }).reset_index()

        listDf = df.copy()

        # Split lists into individual columns
        playersCols = ['position', 'displayName', 'x', 'y', 's', 'a', 'dis', 'o', 'dir']
        allPlayerCols = []
        for col in playersCols:
            allPlayerCol = []
            for i in range(1, 12):
                allPlayerCol.append(f'{col}{i}')
            allPlayerCols.append(allPlayerCol)

        for i in range(len(playersCols)):
            curPlayerCols = allPlayerCols[i]
            df[curPlayerCols] = pd.DataFrame(df[playersCols[i]].tolist(), index=df.index)

        # Reorganize player info columns
        cols = []
        for i in range(1, 12):
            cols.extend([f'{col}{i}' for col in playersCols])

        df = df.drop(columns=playersCols)
        df = df[[
            'uniqueId', 'gameId', 'playId', 'team', 'offenseFormation', 'playDirection',
            'down', 'yardsToGo', 'absoluteYardlineNumber', 'quarter', 'gameClock',
            'winningBy', 'year']
            + cols + [
            'playType'
        ]]

        return df, listDf
        
    def _removeData(self, df):
        playerCols = [f'{col}{i}' for col in ['position', 'displayName', 's', 'a', 'dis'] for i in range(1, 12)]
        df = df.drop(columns=[
            'uniqueId', 'gameId', 'playId', 'team', 'year'
        ] + playerCols)
        return df
    
    def _removePlayInfo(self, df):
        df = df.drop(columns=[
            'offenseFormation', 'playDirection', 'down', 'yardsToGo', 'absoluteYardlineNumber',
            'quarter', 'gameClock', 'winningBy'
        ])
        return df

    def prep(self):
        print('Prepping data...')
        self.masterDf = self._removeDefense(self.masterDf)
        self.masterDf.to_csv('data/prep/master.csv', index=False)
        plays = self._getPlays(self.masterDf)
        plays.to_csv('data/prep/plays.csv', index=False)

        df, listDf = self._combinePlayerLocsPerPlay(self.masterDf.copy())
        df.to_csv('data/prep/playerLocsPerPlay.csv', index=False)

        df = self._removeData(df)
        df.to_csv('data/prep/prepped.csv', index=False)

        df = self._removePlayInfo(df)
        df.to_csv('data/prep/locs.csv', index=False)

        # TODO: Change direction of 3D data so that each layer is an attribute of a player rather than a player
        # This should result in 11 rows per play, and 4 layers
        # rather than 4 rows per play, and 11 layers
        listDf = self._removePlayInfo(listDf)
        listDf.drop(columns=[
            'uniqueId', 'gameId', 'playId', 'team', 'year',
            'position', 'displayName', 's', 'a', 'dis'
        ], inplace=True)
        listDf.to_csv('data/prep/listLocs.csv', index=False)

        print('Data prepped.')



if __name__ == '__main__':
    prepData = PrepData()
    prepData.prep()