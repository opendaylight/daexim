Daexim
======

https://wiki.opendaylight.org/view/Daexim:Main


Usage
-----

    mvn clean install

    karaf/target/assembly/bin/karaf

    opendaylight-user@root>feature:install odl-daexim-all

    opendaylight-user@root>log:tail

    cat > /tmp/daexim-schedule-export-POST.json <<EOL
    {
      "input": {
        "data-export-import:run-at": "1"
      }
    }
    EOL

    curl -u admin:admin --header Content-Type:application/json --data @/tmp/daexim-schedule-export-POST.json http://localhost:8181/restconf/operations/data-export-import:schedule-export

    ls karaf/target/assembly/daexim/

    less karaf/target/assembly/daexim/odl_backup_operational.json
