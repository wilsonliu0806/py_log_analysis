
def main(argv):
    try:
        opts,args = getopt.getopt(argv[1:],"hi:",["help","import"])
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    table_name=""
    for o,a in opts:
        if o in ("-h","--help"):
            useage()
            sys.exit()
        elif o in ("-i","--import"):
            table_name = a
            print("output %s"%table_name)
    if len(table_name) >0:
        if table_name in "access_log_raw":
            print("import start! ")

if __name__ == '__main__':
    print(__name__)
    print(__package__)
    #import_access_log()
    analysis(