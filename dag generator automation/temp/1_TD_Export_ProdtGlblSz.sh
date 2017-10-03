#!/bin/bash
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {{USER}}@{{HOST}} "sh /home/edwext/scripts/fullload/digenexport_full_preprod.ksh 4000 600"
