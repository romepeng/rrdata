from rrdata.common import save_df_to_pgsql, engine
import rrdata.rrdatad.index  as index


def save_index_sw_class_pgsql(table_name="swl_list"):
    data = index.sw_index_class_all()
    #print(data)
    save_df_to_pgsql(data, table_name, engine)
    #print(read_data_from_pg("swl_list"))

def save_index_sw_spot(table_name="swl_spot", level ="L1"):
    """ save swl index L1, L2 realtime price
    """
    for l in ["L1", "L2"]:
        data = index.sw_index_spot(l)
        save_df_to_pgsql(data, f"{table_name}_{l}", engine) 

save_index_sw_class_pgsql()
save_index_sw_spot()


