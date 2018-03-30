#!/usr/bin/python
from kazoo.client import KazooClient #Zookeeper client class
import kazoo #Zookeeper class
import sys #Used to exit properly
import os #Used to check dir existence and chown environment
import pwd #check if user exists
import grp #check group of user
import shutil #used for copying
import argparse

zk_base_path = '/puppet'
default_environment ='defaults'
zk_defaultenv_path = zk_base_path + '/' + default_environment
puppet_environment_base = '/etc/puppetlabs/code/environments'

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

  parser = argparse.ArgumentParser(description='Create puppet environment in Zookeeper and on puppetmaster')
  parser.add_argument('--host', dest='zkhost', required=True, help='Zookeeper host to use for writing')
  parser.add_argument('--environment', dest='environment', required=True, help='environment to create')
  parser.add_argument('--user', dest='user', required=True, help='user for which to create the environment (folder privileges)')
  parser.parse_args(namespace=args)

  #move args to local variables for readability
  zkhost = args.zkhost
  puppet_environment = args.environment
  puppet_environment_directory = puppet_environment_base + '/' + puppet_environment
  ipa_user = args.user

  try:
    user_details = pwd.getpwnam(ipa_user)
  except KeyError:
    print("User '%s' doesn't exist. Please create the user in IPA first!" %ipa_user)
    sys.exit(1)
  #get group of user
  user_group = grp.getgrgid(user_details.pw_gid)
  if os.path.isdir(puppet_environment_directory):
    if query_yes_no("\nEnvironment '%s' allready exists at this location: %s\n\nDo you wish to overwrite it?" % (puppet_environment, puppet_environment_directory)):
      try:
        shutil.rmtree(puppet_environment_directory)
      except OSError:
        print("Unable to delete the folder '%s'. Privileges?" % puppet_environment_directory)
        sys.exit(1)
    else:
      print("Cannot continue. First clear the environment")
      sys.exit(1)
  shutil.copytree(puppet_environment_base + '/production', puppet_environment_directory, ignore=shutil.ignore_patterns('.git', '.librarian', '.tmp', '*.lock', '.gitignore'))

  if os.environ['USER'] != 'root':
    print("Can NOT chown on new environment '%s' on location: %s\nExecute this command manually: 'sudo chown %s:%s %s'" % (puppet_environment, puppet_environment_directory, ipa_user, user_group.gr_name, puppet_environment_directory))
    print("If you don't want to do this manually in the future, run this script as root (or sudo).")
  else:
    os.chown(puppet_environment_directory, user_details.pw_uid, user_details.pw_gid)
    for root, dirs, files in os.walk(puppet_environment_directory):
      for dir in dirs:
        os.chown(os.path.join(root, dir), user_details.pw_uid, user_details.pw_gid)
      for file in files:
        os.chown(os.path.join(root, file), user_details.pw_uid, user_details.pw_gid)

  zk = kazoo.client.KazooClient(hosts=zkhost)
  zk.start()

  try:
    zk.get(zk_base_path + '/' + puppet_environment)
    if query_yes_no("\nEnvironment '%s' allready exists in Zookeeper.\n\nDo you wish to reset it to the defaults?" % puppet_environment):
      zk.delete(zk_base_path + '/' + puppet_environment, recursive=True)
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
