#!/usr/bin/python
from kazoo.client import KazooClient #Zookeeper client class
import kazoo #used to catch exceptions
import yaml
import argparse
import sys

zk_base_path = '/puppet'
default_environment ='defaults'

def query_yes_no(question, default="yes"):
  """Ask a yes/no question via raw_input() and return their answer.

  "question" is a string that is presented to the user.
  "default" is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning
    an answer is required of the user).

  The "answer" return value is True for "yes" or False for "no".
  """
  valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
  if default is None:
    prompt = " [y/n] "
  elif default == "yes":
    prompt = " [Y/n] "
  elif default == "no":
    prompt = " [y/N] "
  else:
    raise ValueError("invalid default answer: '%s'" % default)

  while True:
    sys.stdout.write(question + prompt)
    choice = raw_input().lower()
    if default is not None and choice == '':
      return valid[default]
    elif choice in valid:
      return valid[choice]
    else:
      sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

def main():
  class args(object):
    pass

  parser = argparse.ArgumentParser(description='Fill Zookeeper defaults')
  parser.add_argument('--host', dest='zkhost', required=True, help='Zookeeper host to use for writing')
  parser.add_argument('--osfamily', dest='osfamily', default='', help='OSFamily to set defaults of')
  parser.add_argument('--path', dest='yamllocation', default='.', help='Location of the YAML files')
  parser.parse_args(namespace=args)

  #move args to local variables for readability
  zkhost = args.zkhost
  osfamily = args.osfamily
  yamllocation = args.yamllocation.rstrip('/')

  if osfamily:
    zk_path = zk_base_path + '/' + default_environment + '/' + osfamily
    yamlfile = '%s/defaults_%s.yaml' % (yamllocation, osfamily)
  else:
    zk_path = zk_base_path + '/' + default_environment
    yamlfile = '%s/defaults.yaml' % (yamllocation)

  zk = KazooClient(hosts=zkhost)
  zk.start()
  try:
    zk.get(zk_base_path + '/' + default_environment)
    if query_yes_no("The puppet defaults allready exist. Do you wish to override them with the new defaults from '%s'?" % yamlfile):
      zk.delete(zk_base_path + '/' + default_environment, recursive=True)
      raise kazoo.exceptions.NoNodeError
  except kazoo.exceptions.NoNodeError:
    data = {}
    try:
      #read yaml as dict
      with open(yamlfile, 'r') as stream:
        data = yaml.load(stream)
      #iterate throug yaml/dict
      for k,v in data.iteritems():
        zk.ensure_path(zk_path + '/' + k)
        if type(v) is dict:
          v = yaml.dump(data[k], default_flow_style = False)
        zk.set(zk_path + '/' + k, str(v))
    except kazoo.exceptions.NoNodeError:
      print "Node '%s' doesn't exits" % zk_path
    except IOError:
      print "File not found. Make sure the file '%s' exists in the folder and is readable" % yamlfile

  zk.stop()
  zk.close()

if __name__ == '__main__':
  main()
