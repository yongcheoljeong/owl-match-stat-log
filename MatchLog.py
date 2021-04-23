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

        # def join_playerstatus_and_phs():
        #     self.df_playerstatus = self.df_playerstatus.sort_values(by='time')
        #     self.df_phs = self.df_phs.sort_values(by='time')
        #     merge1 = pd.merge_asof(self.df_playerstatus, self.df_phs, on='time', by=['team_name', 'player_name', 'hero_name'], tolerance=pd.Timedelta('3s'))  

        #     return merge1 
        
        # merge1 = join_playerstatus_and_phs()

        # # join dataframes (playerstatus + phs + gameinfo)
        # def join_merge1_and_gameinfo(merge1):
        #     '''
        #     GameInfo: round 바껴도 round_name이 바뀌지 않는 문제가 있음. round 끝나서 점수 오르고 num_round 바껴도 느려지는 마지막 그 시간에 캐릭터들 위치한 곳이 이전 맵이라서 이런 현상 있는 듯.
        #     '''
        #     self.df_gameinfo = self.df_gameinfo.sort_values(by='time')
        #     merge2 = pd.merge_asof(merge1, self.df_gameinfo, on='time', by=['esports_match_id', 'num_map', 'map_name', 'map_type'], tolerance=pd.Timedelta('2s'))

        #     num_control_maps = self.df_gameinfo[(self.df_gameinfo['map_type'] == 'CONTROL')]['num_map'].unique()

        #     for control_map in num_control_maps:
        #         num_rounds = self.df_gameinfo[self.df_gameinfo['num_map'] == control_map]['num_round'].unique()
        #         for num_round in num_rounds:
        #             dropping_index = merge2[(merge2['num_map'] == control_map) & (merge2['map_type'] == 'CONTROL') & (merge2['num_round'] == num_round) & (merge2['round_name'] != self.df_gameinfo[(self.df_gameinfo['num_map'] == control_map) & (self.df_gameinfo['map_type'] == 'CONTROL') & (self.df_gameinfo['num_round'] == num_round) & (self.df_gameinfo['context'] == 'ROUND_END')]['round_name'].unique()[0])].index
        #             merge2.drop(dropping_index, inplace=True) # drop 'num_round'와 round_name 일치하지 않는 row
        #             merge2.loc[(merge2['num_map'] == control_map) & (merge2['map_type'] == 'CONTROL') & (merge2['num_round'] == num_round), 'round_name'] = self.df_gameinfo[(self.df_gameinfo['num_map'] == control_map) & (self.df_gameinfo['map_type'] == 'CONTROL') & (self.df_gameinfo['num_round'] == num_round) & (self.df_gameinfo['context'] == 'ROUND_END')]['round_name'].unique()[0]

        #     merge2 = merge2.dropna(subset=['num_round']) # num_round == Nan 일 때 row drop

        #     return merge2

        # merge2 = join_merge1_and_gameinfo(merge1) 

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

        merge3 = pd.DataFrame()
        for idx in self.df_roundstart.index:
            ingamedata = merge2.set_index('time')[self.df_roundstart.loc[idx, 'time_start']:self.df_roundstart.loc[idx, 'time_end']]
            ingamedata = ingamedata.reset_index()
            merge3 = pd.concat([merge3, ingamedata], ignore_index=True)

        merge_final = merge3.rename(columns=Match_Scrim_Trans_Info.header_match_to_scrim)

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
    
    def update_FinalStat_to_sql(self):
        pass
        # def get_filelist_all(): 
        #     # set path
        #     filepath = r'G:/공유 드라이브/NYXL Scrim Log/Csv/'
        #     filelist = os.listdir(filepath)
        #     csv_filelist = [x for x in filelist if x.endswith('.csv')]

        #     return csv_filelist
            
        # def get_filelist_updated():
        #     filepath = r'G:/공유 드라이브/NYXL Scrim Log/Csv/'
        #     updated_csv = 'FilesUpdated_FinalStat_MySQL.txt'
        #     f = open(os.path.join(filepath, updated_csv), 'r+')
        #     lines = f.readlines()
        #     updated_filelist = []

        #     for line in lines:
        #         updated_filelist.append(line.replace('\n', ''))
            
        #     f.close()
            
        #     return updated_filelist

        # csv_filelist = get_filelist_all() # all filelist
        # updated_filelist = get_filelist_updated() # updated filelist

        # # sort files to be updated
        # csv_filelist_to_update = list(set(csv_filelist) - set(updated_filelist))
        # csv_filelist_to_update.sort()

        # # export and write
        # filepath = r'G:/공유 드라이브/NYXL Scrim Log/Csv/'
        # updated_csv = 'FilesUpdated_FinalStat_MySQL.txt'
        # f = open(os.path.join(filepath, updated_csv), 'a')
        # for filename in csv_filelist_to_update:
        #     scrimlog = ScrimLog(filename)
        #     df_sql = MySQLConnection(input_df=scrimlog.df_FinalStat.reset_index(), dbname='scrim_finalstat') # reset_index to export to mysql db
        #     table_name = scrimlog.match_id.split('.csv')[0] # drop '.csv' as a table_name
        #     df_sql.export_to_db(table_name=table_name, if_exists='replace')

        #     f.write(filename+'\n')
        #     print(f'File Exported to {df_sql.dbname}: {filename}')
        # f.close()