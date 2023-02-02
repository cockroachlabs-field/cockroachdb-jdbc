#!/bin/bash
# Maven integration tests menu

set -e

case "$OSTYPE" in
  darwin*)
        default="\x1B[0m"
        cyan="\x1B[36m"
        yellow="\x1B[33m"
        magenta="\x1B[35m"
        creeol="\r\033[K"
        ;;
  *)
        default="\e[0m"
        cyan="\e[36m"
        yellow="\e[33m"
        magenta="\e[35m"
        creeol="\r\033[K"
        ;;
esac

fn_echo(){
  echo -en "${creeol}${cyan}$@${default}"
	echo -en "\n"
}

fn_select_group() {
  PS3='Please enter integration test groups (-Dgroups): '
  options=("retry-test" "batch-test" "anomaly-test" "QUIT")
  select opt in "${options[@]}"
  do
      case $opt in
          "retry-test")
              groups=$opt
              break
              ;;
          "batch-test")
              groups=$opt
              break
              ;;
          "anomaly-test")
              groups=$opt
              break
              ;;
          "QUIT")
              exit 0
              ;;
          *) echo "invalid option $REPLY";;
      esac
  done

  fn_echo "Selected groups: $groups"
}

fn_select_profile() {
  PS3='Please enter test profile (-P): '
  options=("it" "it-dev" "QUIT")
  select opt in "${options[@]}"
  do
      case $opt in
          "it")
              profile=$opt
              fn_select_group
              break
              ;;
          "it-dev")
              profile=$opt
              fn_select_group
              break
              ;;
          "QUIT")
              exit 0
              ;;
          *) echo "invalid option $REPLY";;
      esac
  done

  fn_echo "Selected profile: $profile"
}

################################
################################
################################

profile=it
groups=

fn_echo "** Test Menu **"

fn_select_profile

./mvnw -P "$profile" -Dgroups="$groups" clean install
