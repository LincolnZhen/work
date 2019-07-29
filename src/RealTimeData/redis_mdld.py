import yaml
from redis_flist import Config


def init_slist(filename:str):
    with open(filename,'r') as file:
        cont = file.read()
        res = yaml.load(cont,Loader=yaml.FullLoader)
        return  res['config']['slist']

def main():
    config = Config(config_slist=init_slist("redis_mdld.yaml"))
    print(type(config.config_slist))

if __name__ == '__main__':
    main()
