from rrdata.utils.rqPgsql import save_data_to_postgresql, read_data_from_pg
import rrdata.rrdatad.index  as index


def save_index_sw_class_pgsql(table_name="swl_list", db_name="rrdata"):
    data = index.sw_index_class_all()
    print(data)
    save_data_to_postgresql(data, table_name, db_name)
    #print(read_data_from_pg("swl_list"))

if __name__ == '__main__':
    save_index_sw_class_pgsql()


