#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import numpy as np
import datetime as dt


def gs_select_change_names_mapis(file): # Group Sales data prepare from MAPIS 
    df1 = pd.read_excel(file)
    df =df1[["VIN","Volume: Group Sales"]]
    df.rename(columns = {"Volume: Group Sales":'Sales',}, inplace = True)  
    return df

def gs_select_change_names_swt(file):   # Group Sales data prepare from SAP SWT
    df1 = pd.read_excel(file,skiprows=2)
    df =df1[["VIN","GS Sales Volume","COMMISSION_NUM"]].rename(columns = {"GS Sales Volume":'Sales'})
    df_po_vin_uni = df[["VIN",'COMMISSION_NUM']].dropna().drop_duplicates()
    return df,df_po_vin_uni

def ws_select_change_names_mapis(file):# Wholesales data prepare from MAPIS 
    df1 = pd.read_excel(file)
    df =df1[["VIN","Volume: Wholesale net"]]
    df.rename(columns = {"Volume: Wholesale net":'Sales'}, inplace = True)
    return df

def ws_select_change_names_swt(file):  # Wholesales data prepare from SAP SWT
    df1 = pd.read_excel(file,skiprows=2)
    df =df1[["VIN","WS Sales Volume","COMMISSION_NUM"]].rename(columns = {"WS Sales Volume":'Sales'})
    df_po_vin_uni = df[["VIN",'COMMISSION_NUM']].dropna().drop_duplicates()
    return df,df_po_vin_uni

def retail_select_change_names_mapis(file):# Retails data prepare from MAPIS
    df1 = pd.read_excel(file)
    df = df1[['VIN','Volume: Retail Sales net']]
    df.rename(columns = {"Volume: Retail Sales net":'Sales'}, inplace = True)
    return df

def retail_select_change_names_swt(file): # Retails data prepare from SAP SWT
    df1 = pd.read_excel(file,skiprows=2)
    df =df1[["VIN","RT Sales Volume","SWTRDR_PO"]].rename(columns = {"RT Sales Volume":'Sales',"SWTRDR_PO":'COMMISSION_NUM'})
    df_po_vin_uni = df[["VIN",'COMMISSION_NUM']].dropna().drop_duplicates()
    return df,df_po_vin_uni

def drop_0(df): # filter out 0 values which is not relevant
    binary_list = [1,   -1]
    zfilter = df["Sales"].isin(binary_list)
    ndf = df[zfilter]
    return ndf

def group_vin_sum_not_0(df): # further filter out 0 values after joining data
    df_grouped = df.groupby("VIN")["Sales"].sum().sort_values().reset_index()
    df_notnull = df_grouped [df_grouped ["Sales"]!=0].sort_values("VIN") 
    return df_notnull

def outer_join_filter_com(map1,swt1): # joining data from two systems
    mapis_swt_compare1 = map1.merge(swt1,on="VIN",how="outer",suffixes=('_mapis', '_swt')).fillna(0)
    result = mapis_swt_compare1[mapis_swt_compare1.Sales_mapis!=mapis_swt_compare1.Sales_swt]
    return result

def create_subfolders_ex(subfolder_name): # create folders
        if not os.path.exists(subfolder_name):
            os.makedirs(os.path.join(os.getcwd(), subfolder_name))  


def left_join_for_po(without_po,withpo):  # adding more production info 
    
    bf_padding = without_po.merge(withpo,on="VIN",how="left")
    bf_padding['COMMISSION_NUM'] =bf_padding['COMMISSION_NUM'].astype(int).astype(str).str.zfill(10)
    
    return  bf_padding      
  
def main():
    opath = r"\\s173mho1fs2\E$\Share\DIST\Dist_Internal\AIDA\Python_Reconcilation_AIDA"
    os.chdir(opath)
    today = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d')    
    #1st read in data and select and change VIN and Sales names
    for path, dirs, filenames in os.walk(opath):
        for f in filenames:
            if f.endswith('.xlsx') and (("RS") in f):
                if (("MAPIS") in f): 
                    rs_map =retail_select_change_names_mapis(f)
                if (("SWT") in f): 
                    rs_swt,rs_polist =retail_select_change_names_swt(f)            
            if f.endswith('.xlsx') and (("WS") in f):
                if (("MAPIS") in f): 
                    ws_map =ws_select_change_names_mapis(f)
                if (("SWT") in f): 
                    ws_swt,ws_polist =ws_select_change_names_swt(f)    
            if f.endswith('.xlsx') and (("GS") in f):
                if (("MAPIS") in f): 
                    gs_map =gs_select_change_names_mapis(f)
                if (("SWT") in f): 
                    gs_swt,gs_polist =gs_select_change_names_swt(f)               
    
    # 2nd filter 0 out of all the swt data
    rs_swt_bi = drop_0(rs_swt)
    ws_swt_bi = drop_0(ws_swt)
    gs_swt_bi = drop_0(gs_swt)
    
    # 3rd group_vin_sum_not_0
    not0_rs_map = group_vin_sum_not_0(rs_map)
    not0_ws_map = group_vin_sum_not_0(ws_map)
    not0_gs_map = group_vin_sum_not_0(gs_map)
    not0_rs_swt = group_vin_sum_not_0(rs_swt_bi)
    not0_ws_swt = group_vin_sum_not_0(ws_swt_bi)
    not0_gs_swt = group_vin_sum_not_0(gs_swt_bi)
    
    # 4th outer_join_filter_com
    result_rs_0 = outer_join_filter_com(not0_rs_map,not0_rs_swt)
    result_gs_0 = outer_join_filter_com(not0_ws_map,not0_ws_swt)
    result_ws_0 = outer_join_filter_com(not0_gs_map,not0_gs_swt)
    
    result_rs =  left_join_for_po(result_rs_0,rs_polist)
    result_gs =  left_join_for_po(result_gs_0,gs_polist)
    result_ws =  left_join_for_po(result_ws_0,ws_polist)
    
    # 5th create a new folder 
    create_subfolders_ex("PICKUP")
    os.chdir("PICKUP")
    
    # 6th export to Excel files to the folder 
    result_rs.to_excel(f"Retail_problematic_VINs_{today}.xlsx",index=False)
    result_gs.to_excel(f"GroupSale_problematic_VINs_{today}.xlsx",index=False)
    result_ws.to_excel(f"WholeSale_problematic_VINs_{today}.xlsx",index=False)
    print ("Success")
    print ("Success")

if __name__=="__main__":
    main()


