import pandas as pd 
import numpy as np 
import os 
from TraditionalStat import *
from AdvancedStat import * 
from TeamfightDetector import *
from PeriEventTimeHistogram import *
from MySQLConnection import *
import mysql_auth 
import Match_Scrim_Trans_Info

class MatchLog():
    def __init__(self, match_id=None):
        if match_id is None:
            pass 
        else:
            self.match_id = match_id
            self.set_gameinfo()
            self.set_roundstart()
            self.set_gameresult()
            self.set_playerstatus()
            self.set_phs()
            self.set_df_input()
            self.set_index()
            self.set_WorkshopStat()
            self.set_TraditionalStat()
            self.set_AdvancedStat()
            self.set_TeamfightDetector()
            self.set_FinalStatIndex()

    def set_gameinfo(self):
        table_name = f'match_{self.match_id}'
        login_info = mysql_auth.NYXLDB_ESD_GameInfo
        dbname = login_info['dbname']
        
        gameinfo = MySQLConnection(login_info=login_info)
        sql = f"SELECT * FROM {dbname}.`{table_name}`;"
        df_gameinfo = gameinfo.import_db_by_sql(sql=sql)

        df_gameinfo['time'] = pd.to_datetime(df_gameinfo['time'], utc=True, unit='ms')
        self.df_gameinfo = df_gameinfo

    def set_roundstart(self):
        table_name = f'match_{self.match_id}'
        login_info = mysql_auth.NYXLDB_ESD_RoundStart
        dbname = login_info['dbname']
        
        roundstart = MySQLConnection(login_info=login_info)
        sql = f"SELECT * FROM {dbname}.`{table_name}`;"
        df_roundstart = roundstart.import_db_by_sql(sql=sql)

        df_roundstart['time_start'] = pd.to_datetime(df_roundstart['time_start'], utc=True, unit='ms')
        df_roundstart['time_end'] = pd.to_datetime(df_roundstart['time_end'], utc=True, unit='ms')
        self.df_roundstart = df_roundstart

    def set_gameresult(self):
        table_name = f'match_{self.match_id}'
        login_info = mysql_auth.NYXLDB_ESD_GameResult
        dbname = login_info['dbname']
        
        gameresult = MySQLConnection(login_info=login_info)
        sql = f"SELECT * FROM {dbname}.`{table_name}`;"
        df_gameresult = gameresult.import_db_by_sql(sql=sql)

        self.df_gameresult = df_gameresult 

    def set_playerstatus(self):
        table_name = f'match_{self.match_id}'
        login_info = mysql_auth.NYXLDB_ESD_PlayerStatus
        dbname = login_info['dbname']
        
        playerstatus = MySQLConnection(login_info=login_info)
        sql = f"SELECT * FROM {dbname}.`{table_name}`;"
        df_playerstatus = playerstatus.import_db_by_sql(sql=sql)

        df_playerstatus = df_playerstatus.fillna(0)
        df_playerstatus['time'] = pd.to_datetime(df_playerstatus['time'], utc=True, unit='ms')

        self.df_playerstatus = df_playerstatus
    
    def set_kill(self):
        table_name = f'match_{self.match_id}'
        login_info = mysql_auth.NYXLDB_ESD_Kill
        dbname = login_info['dbname']
        
        kill = MySQLConnection(login_info=login_info)
        sql = f"SELECT * FROM {dbname}.`{table_name}`;"
        df_kill = kill.import_db_by_sql(sql=sql)

        df_kill['time'] = pd.to_datetime(df_kill['time'], utc=True, unit='ms')

        self.df_kill = df_kill

    def set_phs(self):
        table_name = f'match_{self.match_id}'
        login_info = mysql_auth.NYXLDB_ESD_PHS
        dbname = login_info['dbname']
        
        phs = MySQLConnection(login_info=login_info)

        def phs_query(dbname, table_name):
            ssg_list = tuple([ssg for stat_name, ssg in Match_Scrim_Trans_Info.ssg_dict.items()])
            IN_cond = f"{ssg_list}"
            stat_lifespan = 'GAME'
            sql = f"SELECT * FROM {dbname}.`{table_name}` WHERE `stat_lifespan` = '{stat_lifespan}' AND `ssg` IN {IN_cond} ORDER BY `time`;"

            return sql

        sql = phs_query(dbname=dbname, table_name=table_name)
        df_phs = phs.import_db_by_sql(sql=sql)

        def pivot_statname(df_phs):
            df_phs = df_phs 
            df_phs.drop(columns='stat_name', inplace=True)
            df_phs_pivot = pd.pivot_table(df_phs, index=['time', 'team_name', 'player_name', 'hero_name'], columns=['ssg'], aggfunc=['mean'])
            df_phs_pivot.columns = df_phs_pivot.columns.droplevel([0, 1]) # droplevel: ['mean', 'amount']
            col_not_in_phs_pivot = list(set(list(Match_Scrim_Trans_Info.ssg_dict.values())) - set(df_phs_pivot.columns))

            if len(col_not_in_phs_pivot) > 0: # EnvironmentalKills 처럼 경기에 하나도 찍히지 않아서 column이 생기지 않는 경우 에러 방지하기 위해 해당 column 임의로 만들고 data 0으로 채움
                for col in col_not_in_phs_pivot:
                    df_phs_pivot[col] = 0

            df_phs_pivot.reset_index(inplace=True)
            
            # transform ssg to stat_name
            ssg_transform = {ssg:stat_name for stat_name, ssg in Match_Scrim_Trans_Info.ssg_dict.items()}
            df_phs_pivot.rename(columns=ssg_transform, inplace=True)

            df_phs_pivot = df_phs_pivot[df_phs_pivot['hero_name'] != 'All Heroes'] # drop where 'All Heroes'
            df_phs_pivot = df_phs_pivot.fillna(0)

            return df_phs_pivot
        
        df_phs = pivot_statname(df_phs)

        df_phs['time'] = pd.to_datetime(df_phs['time'], utc=True, unit='ms')

        self.df_phs = df_phs


    def set_df_input(self):

        def join_phs_and_gameinfo():
            self.df_phs = self.df_phs.sort_values(by='time')
            self.df_gameinfo = self.df_gameinfo.sort_values(by='time')
            df_merge = pd.merge_asof(self.df_phs, self.df_gameinfo, on='time', tolerance=pd.Timedelta('1.5s'))
            
            num_control_maps = self.df_gameinfo[(self.df_gameinfo['map_type'] == 'CONTROL')]['num_map'].unique()

            for control_map in num_control_maps:
                num_rounds = self.df_gameinfo[self.df_gameinfo['num_map'] == control_map]['num_round'].unique()
                for num_round in num_rounds:
                    dropping_index = df_merge[(df_merge['num_map'] == control_map) & (df_merge['map_type'] == 'CONTROL') & (df_merge['num_round'] == num_round) & (df_merge['round_name'] != self.df_gameinfo[(self.df_gameinfo['num_map'] == control_map) & (self.df_gameinfo['map_type'] == 'CONTROL') & (self.df_gameinfo['num_round'] == num_round) & (self.df_gameinfo['context'] == 'ROUND_END')]['round_name'].unique()[0])].index
                    df_merge.drop(dropping_index, inplace=True) # drop 'num_round'와 round_name 일치하지 않는 row
                    df_merge.loc[(df_merge['num_map'] == control_map) & (df_merge['map_type'] == 'CONTROL') & (df_merge['num_round'] == num_round), 'round_name'] = self.df_gameinfo[(self.df_gameinfo['num_map'] == control_map) & (self.df_gameinfo['map_type'] == 'CONTROL') & (self.df_gameinfo['num_round'] == num_round) & (self.df_gameinfo['context'] == 'ROUND_END')]['round_name'].unique()[0]

            df_merge = df_merge.dropna(subset=['num_round']) # num_round == Nan 일 때 row drop

            return df_merge 
        
        merge1 = join_phs_and_gameinfo()

        def join_merge1_and_playerstatus(merge1):
            self.df_playerstatus = self.df_playerstatus.sort_values(by='time')
            merge1['esports_match_id'] = merge1['esports_match_id'].astype(int)
            merge1['num_map'] = merge1['num_map'].astype(int)
            self.df_playerstatus['esports_match_id'] = self.df_playerstatus['esports_match_id'].astype(int)
            self.df_playerstatus['num_map'] = self.df_playerstatus['num_map'].astype(int)
            df_merge = pd.merge_asof(merge1, self.df_playerstatus, on='time', by=['esports_match_id', 'num_map', 'map_name', 'map_type', 'team_name', 'player_name', 'hero_name'], tolerance=pd.Timedelta('1.5s'))  

            return df_merge 
        
        merge2 = join_merge1_and_playerstatus(merge1)

        # slice time to see only in-game data
        merge3 = pd.DataFrame()
        for idx in self.df_roundstart.index:
            ingamedata = merge2.set_index('time')[self.df_roundstart.loc[idx, 'time_start']:self.df_roundstart.loc[idx, 'time_end']]
            ingamedata = ingamedata.reset_index()
            merge3 = pd.concat([merge3, ingamedata], ignore_index=True)

        # merge match result info
        match_result_info = self.df_gameresult[['esports_match_id', 'num_map', 'map_name', 'map_type', 'map_winner']]
        score_info = match_result_info['map_winner'].value_counts()
        match_winner = score_info[score_info == score_info.max()].index[0]
        match_loser = [x for x in merge3['team_name'].unique() if x != match_winner][0]
        winner_match_score = score_info[match_winner]
        loser_match_score = match_result_info['map_winner'].str.count(match_loser).sum()
        match_result_info = match_result_info.copy()
        match_result_info['match_winner'] = match_winner 
        match_result_info['match_loser'] = match_loser 
        match_result_info['winner_match_score'] = winner_match_score 
        match_result_info['loser_match_score'] = loser_match_score 
        match_result_info['esports_match_id'] = match_result_info['esports_match_id'].astype(int)
        match_result_info['num_map'] = match_result_info['num_map'].astype(int)

        merge4 = pd.merge(merge3, match_result_info, how='inner')

        merge_final = merge4.rename(columns=Match_Scrim_Trans_Info.header_match_to_scrim)

        self.df_input = merge_final

    def set_index(self):
        # df_init
        self.df_init = self.df_input 

        # idx_col
        self.idx_col = ['MatchId', 'num_map', 'Map', 'map_type', 'Section', 'RoundName', 'Timestamp', 'Team', 'Player', 'Hero']
    
    def set_WorkshopStat(self):
        df_WorkshopStat = self.df_init.set_index(self.idx_col)

        self.df_WorkshopStat = df_WorkshopStat
    
    def set_TraditionalStat(self):

        # AllDamageDealt
        df_TraditionalStat = AllDamageDealt(self.df_WorkshopStat).get_df_result()
        # HealingReceived
        df_TraditionalStat = HealingReceived(df_TraditionalStat).get_df_result()
        # HealthPercent
        df_TraditionalStat = HealthPercent(df_TraditionalStat).get_df_result()
        # NumAlive
        df_TraditionalStat = NumAlive(df_TraditionalStat).get_df_result()

        # dx
        '''
        현재 스크림 워크샵이 영웅별로 스탯을 누적해주는 것이 아니라 플레이어 별로 스탯을 누적해주기 때문에 선수가 도중에 영웅을 바꿀 경우 diff() 함수에서 문제가 발생 --> `hero_col` 따로 빼서 diff() 구하고 나중에 merge로 해결
        '''
        def diff_stat(df_input=None):
            '''
            dt = 0 이 되는 경우가 있는데 2초에 한번씩 찍히는 PHS와 1초에 한번에 찍히는 playerstatus 간 merging 과정에서 phs의 가운데 값이 똑같이 찍혀서 생기는 오류인가?
            --> PHS + GameInfo merge_asfo 하고 이후에 이걸 playerstatus에 merge_asof 하면 되지 않을까?
            '''
            diff_stat_list = [stat_name for stat_name, ssg in Match_Scrim_Trans_Info.ssg_dict.items()] # define stat names to diff()
            df_stats = df_input[diff_stat_list]

            dx = df_stats.groupby(level=['MatchId', 'num_map', 'Map', 'map_type', 'Section', 'RoundName', 'Team', 'Player', 'Hero']).diff()
            dx = dx.fillna(0)
            dt = dx.reset_index('Timestamp').groupby(level=['MatchId', 'num_map', 'Map', 'map_type', 'Section', 'RoundName', 'Team', 'Player', 'Hero'])['Timestamp'].diff().dt.total_seconds().values
            dxdt = dx.div(dt, axis=0)
            dxdt = dxdt.fillna(0)

            def set_dx(dx):
                '''
                FinalBlows, Deaths와 같은 Event들은 dxdt가 아니라 dx로 접근해야 정확히 파악 가능
                column 이름은 정확히 따지자면 '/2s' 혹은 '_dx'가 돼야하지만 AdvancedStat 계산 등 이후 코드들이 '/s'로 통일돼있기 때문에 그냥 진행
                '''
                self.dx_stat_list = ['Deaths', 'Eliminations', 'FinalBlows', 'EnvironmentalDeaths', 'EnvironmentalKills', 'ObjectiveKills', 'SoloKills', 'UltimatesEarned', 'UltimatesUsed', 'DefensiveAssists', 'OffensiveAssists']
                dx = dx[self.dx_stat_list]

                return dx

            def set_dxdt(dxdt):
                '''
                HeroDamageDealt, HealingDealt와 같은 연속값들은 dxdt로 접근해야 정확히 파악 가능
                '''
                self.dxdt_stat_list = ['HeroDamageDealt', 'BarrierDamageDealt', 'HeroDamageTaken', 'HealingDealt', 'HealingReceived']
                dxdt = dxdt[self.dxdt_stat_list]

                return dxdt

            dx = set_dx(dx)
            dxdt = set_dxdt(dxdt)

            dxdt_merge = pd.merge(dx, dxdt, how='outer', left_index=True, right_index=True)

            df_merge = pd.merge(df_input, dxdt_merge, how='left', left_index=True, right_index=True, suffixes=('', '/s'))

            return df_merge
        
        # calculate dx table and merge
        df_TraditionalStat = diff_stat(df_input=df_TraditionalStat)

        self.df_TraditionalStat = df_TraditionalStat

    def set_AdvancedStat(self):
        # RCP
        df_AdvancedStat = RCPv1(self.df_TraditionalStat).get_df_result()
        # FB_value
        df_AdvancedStat = FBValue(df_AdvancedStat).get_df_result()
        # Death_risk
        df_AdvancedStat = DeathRisk(df_AdvancedStat).get_df_result()
        # New AdvancedStat here

        self.df_AdvancedStat = df_AdvancedStat
    
    def set_TeamfightDetector(self):
        numMaps = self.df_AdvancedStat.index.unique(level='num_map')
        df_TFStat = pd.DataFrame()
        for numMap in numMaps:
            df_TFbyMap = TeamfightDetector(self.df_AdvancedStat.xs(numMap, level='num_map', drop_level=False)).get_df_result()

            df_TFStat = pd.concat([df_TFStat, df_TFbyMap])

        # indexing
        df_TFStat = df_TFStat.groupby(by=self.idx_col).max()
        
        # DominanceIndex
        df_TFStat = DIv2(df_TFStat).get_df_result()

        self.df_TFStat = df_TFStat
    
    def set_FinalStatIndex(self):
        df_FinalStat = self.df_TFStat.reset_index()
        self.df_FinalStat = df_FinalStat.groupby(by=self.idx_col).max()

    def get_df_FinalStat(self):
        return self.df_FinalStat
    
    def export_to_db(self, if_exists='replace'):
        df_FinalStat = self.get_df_FinalStat()
        login_info = mysql_auth.NYXLDB_ESD_FinalStat
        sql_con = MySQLConnection(input_df=df_FinalStat, login_info=login_info)
        sql_con.export_to_db(table_name=f'match_{self.match_id}', if_exists=if_exists) # export
        print(f'FinalStat Exported: match_{self.match_id}')
    
    def update_FinalStat_to_sql(self, if_exists='pass'): # update tables
        def get_all_matchlist(): 
            gameinfo_list = MySQLConnection(login_info=mysql_auth.NYXLDB_ESD_GameInfo).get_table_names()
            playerstatus_list = MySQLConnection(login_info=mysql_auth.NYXLDB_ESD_PlayerStatus).get_table_names()
            phs_list = MySQLConnection(login_info=mysql_auth.NYXLDB_ESD_PHS).get_table_names()
            gameresult_list = MySQLConnection(login_info=mysql_auth.NYXLDB_ESD_GameResult).get_table_names()
            roundstart_list = MySQLConnection(login_info=mysql_auth.NYXLDB_ESD_RoundStart).get_table_names()

            all_matchlist = list(set(gameinfo_list) & set(playerstatus_list) & set(phs_list) & set(gameresult_list) & set(roundstart_list))
            all_matchlist2 = []

            for match_id in all_matchlist:
                match_id2 = match_id.split('_')[1]
                all_matchlist2.append(match_id2)

            return all_matchlist2
            
        def get_updated_matchlist():
            updated_match_list = MySQLConnection(login_info=mysql_auth.NYXLDB_ESD_FinalStat).get_table_names()
            updated_match_list2 = []

            for match_id in updated_match_list:
                match_id2 = match_id.split('_')[1]
                updated_match_list2.append(match_id2)
            
            return updated_match_list2
        
        if if_exists == 'pass':
            all_matchlist = get_all_matchlist()
            updated_matchlist = get_updated_matchlist()
            matchlist_to_update = list(set(all_matchlist) - set(updated_matchlist))

            for match_id in matchlist_to_update:
                matchlog = MatchLog(match_id=match_id)
                df_sql = MySQLConnection(input_df=matchlog.df_FinalStat.reset_index(), login_info=mysql_auth.NYXLDB_ESD_FinalStat)
                table_name = f'match_{matchlog.match_id}'
                df_sql.export_to_db(table_name=table_name, if_exists='replace')

                print(f'FinalStat Exported: {table_name}')
        
        elif if_exists == 'replace':
            all_matchlist = get_all_matchlist()
            matchlist_to_update = all_matchlist 

            for match_id in matchlist_to_update:
                matchlog = MatchLog(match_id=match_id)
                df_sql = MySQLConnection(input_df=matchlog.df_FinalStat.reset_index(), login_info=mysql_auth.NYXLDB_ESD_FinalStat)
                table_name = f'match_{matchlog.match_id}'
                df_sql.export_to_db(table_name=table_name, if_exists='replace')

                print(f'FinalStat Exported: {table_name}')
