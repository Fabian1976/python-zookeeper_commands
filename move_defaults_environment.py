#!/usr/bin/python
from kazoo.client import KazooClient #Zookeeper client class
import kazoo #Zookeeper class
import argparse
import sys

zk_base_path = '/puppet'
default_environment ='defaults'
zk_defaultenv_path = zk_base_path + '/' + default_environment

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

  parser = argparse.ArgumentParser(description='Move defaults to an environment')
  parser.add_argument('--host', dest='zkhost', required=True, help='Zookeeper host to use for writing')
  parser.add_argument('--environment', dest='environment', required=True, help='environment to copy defaults to')
  parser.parse_args(namespace=args)

  #move args to local variables for readability
  zkhost = args.zkhost
  puppet_environment = args.environment

  zk = kazoo.client.KazooClient(hosts=zkhost)
  zk.start()

  try:
    zk.get(zk_base_path + '/' + puppet_environment + '/' + default_environment)
    if query_yes_no("The entry defaults allready exist in environment '%s'. Do you wish to override it with the new defaults?" % puppet_environment):
      zk.delete(zk_base_path + '/' + puppet_environment + '/' + default_environment, recursive=True)
      raise kazoo.exceptions.NoNodeError
  except kazoo.exceptions.NoNodeError:
    try:
      children = zk.get_children(zk_defaultenv_path)
      for child in children:
        data, stat = zk.get(zk_defaultenv_path + '/' + child)
        if data.decode('utf-8') == '':
          # empty node (probably has childs)
          subchildren = zk.get_children(zk_defaultenv_path + '/' + child)
          for subchild in subchildren:
            subdata, substat = zk.get(zk_defaultenv_path + '/' + child + '/' + subchild)
            print "%s::%s: %s" % (child, subchild, subdata.decode("utf-8"))
            # create path for new environment
            zk.ensure_path(zk_base_path + '/' + puppet_environment + '/' + default_environment + '/' + child + '/' + subchild)
            zk.set(zk_base_path + '/' + puppet_environment + '/' + default_environment + '/' + child + '/' + subchild, subdata)
        else:
          print "%s: %s" % (child, data.decode("utf-8"))
          # create path for new environment
          zk.ensure_path(zk_base_path + '/' + puppet_environment + '/' + default_environment + '/' + child)
          zk.set(zk_base_path + '/' + puppet_environment + '/' + default_environment + '/' + child, data)
    except kazoo.exceptions.NoNodeError:
      print "Node '%s' doesn't exits" % zk_defaultenv_path

  zk.stop()
  zk.close()

if __name__ == '__main__':
  main()
