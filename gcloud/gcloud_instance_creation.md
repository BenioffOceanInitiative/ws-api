
## command line

```bash
gcloud beta compute --project=benioff-ocean-initiative instances create ws-api --zone=us-west2-a --machine-type=e2-standard-2 --subnet=default --network-tier=PREMIUM --maintenance-policy=MIGRATE --service-account=p3test@benioff-ocean-initiative.iam.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --tags=http-server,https-server --image=ubuntu-2004-focal-v20200423 --image-project=ubuntu-os-cloud --boot-disk-size=40GB --boot-disk-type=pd-ssd --boot-disk-device-name=ws-api --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --reservation-affinity=any

gcloud compute --project=benioff-ocean-initiative firewall-rules create default-allow-http --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:80 --source-ranges=0.0.0.0/0 --target-tags=http-server

gcloud compute --project=benioff-ocean-initiative firewall-rules create default-allow-https --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:443 --source-ranges=0.0.0.0/0 --target-tags=https-server
```

## REST

```
POST https://www.googleapis.com/compute/v1/projects/benioff-ocean-initiative/zones/us-west2-a/instances
{
  "kind": "compute#instance",
  "name": "ws-api",
  "zone": "projects/benioff-ocean-initiative/zones/us-west2-a",
  "machineType": "projects/benioff-ocean-initiative/zones/us-west2-a/machineTypes/e2-standard-2",
  "displayDevice": {
    "enableDisplay": false
  },
  "metadata": {
    "kind": "compute#metadata",
    "items": []
  },
  "tags": {
    "items": [
      "http-server",
      "https-server"
    ]
  },
  "disks": [
    {
      "kind": "compute#attachedDisk",
      "type": "PERSISTENT",
      "boot": true,
      "mode": "READ_WRITE",
      "autoDelete": true,
      "deviceName": "ws-api",
      "initializeParams": {
        "sourceImage": "projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20200423",
        "diskType": "projects/benioff-ocean-initiative/zones/us-west2-a/diskTypes/pd-ssd",
        "diskSizeGb": "40"
      },
      "diskEncryptionKey": {}
    }
  ],
  "canIpForward": false,
  "networkInterfaces": [
    {
      "kind": "compute#networkInterface",
      "subnetwork": "projects/benioff-ocean-initiative/regions/us-west2/subnetworks/default",
      "accessConfigs": [
        {
          "kind": "compute#accessConfig",
          "name": "External NAT",
          "type": "ONE_TO_ONE_NAT",
          "networkTier": "PREMIUM"
        }
      ],
      "aliasIpRanges": []
    }
  ],
  "description": "",
  "labels": {},
  "scheduling": {
    "preemptible": false,
    "onHostMaintenance": "MIGRATE",
    "automaticRestart": true,
    "nodeAffinities": []
  },
  "deletionProtection": false,
  "reservationAffinity": {
    "consumeReservationType": "ANY_RESERVATION"
  },
  "serviceAccounts": [
    {
      "email": "p3test@benioff-ocean-initiative.iam.gserviceaccount.com",
      "scopes": [
        "https://www.googleapis.com/auth/cloud-platform"
      ]
    }
  ],
  "shieldedInstanceConfig": {
    "enableSecureBoot": false,
    "enableVtpm": true,
    "enableIntegrityMonitoring": true
  }
}

POST https://www.googleapis.com/compute/v1/projects/benioff-ocean-initiative/global/firewalls
{
  "name": "default-allow-http",
  "kind": "compute#firewall",
  "sourceRanges": [
    "0.0.0.0/0"
  ],
  "network": "projects/benioff-ocean-initiative/global/networks/default",
  "targetTags": [
    "http-server"
  ],
  "allowed": [
    {
      "IPProtocol": "tcp",
      "ports": [
        "80"
      ]
    }
  ]
}

POST https://www.googleapis.com/compute/v1/projects/benioff-ocean-initiative/global/firewalls
{
  "name": "default-allow-https",
  "kind": "compute#firewall",
  "sourceRanges": [
    "0.0.0.0/0"
  ],
  "network": "projects/benioff-ocean-initiative/global/networks/default",
  "targetTags": [
    "https-server"
  ],
  "allowed": [
    {
      "IPProtocol": "tcp",
      "ports": [
        "443"
      ]
    }
  ]
}
```
