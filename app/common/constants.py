import pandas as pd
import helper_functions

# Colors
K8S_BLUE = '#3970e4'
TESLA_RED = '#cc0000'
STAFFING_GREEN = '#3cb043'

def get_statics(db, zone):
    query = f"""
        SELECT CARSET_DIVISOR AS divisor
        FROM gf1pe_bm_global._static_lines gb
        WHERE gb.ZONE = {zone} 
        AND gb.CARSET_DIVISOR IS NOT NULL;
    """
    static_df = pd.read_sql(query, db)
    return int(static_df['divisor'].iloc[0])
    
# Stagnant on NORMAL_DIVISOR & CMA_DIVISOR due to possible independence from certain zones.
pedb_con = helper_functions.get_sql_conn('pedb', schema='gf1pe_bm_global')
Z1_DIVISOR = get_statics(pedb_con, zone=1)
Z2_DIVISOR = get_statics(pedb_con, zone=2)
Z3_DIVISOR = get_statics(pedb_con, zone=3)
Z4_DIVISOR = get_statics(pedb_con, zone=4)
Z5_DIVISOR = get_statics(pedb_con, zone=5)
pedb_con.close()
