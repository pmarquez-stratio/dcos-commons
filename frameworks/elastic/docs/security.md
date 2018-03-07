---
layout: layout.pug
navigationTitle:
excerpt:
title: Security
menuWeight: 22
---

{% assign data = site.data.services.elastic %}

# DC/OS {{ data.techName }} Security

The DC/OS {{ data.techName }} service supports {{ data.techName }}'s X-Pack transport encryption mechanisms. The service provides automation and orchestration to simplify the usage of these important features. At this time, X-Pack's authentication and authorization features are not supported.

A good overview of X-Pack can be found [here](https://www.elastic.co/guide/en/x-pack/current/xpack-introduction.html).

*Note*: These security features are only available on DC/OS Enterprise 1.10 and above.

## Transport Encryption

{% include services/security-transport-encryption-lead-in.md data=data %}

*Note*: X-Pack is required to enable Transport Encryption.

{% include services/security-configure-transport-encryption.md data=data %}

*Note* It is possible to enable Transport Encryption after initial installation, but it requires setting `service.update_strategy` to `parallel`. After the update is complete, `service.update_strategy` should be set back to `serial`. Because the update must occur in parallel, the service **WILL** be unavailable during the transition. Additionally, clients will need to be reconfigured after the transition.

{% include services/security-transport-encryption-clients.md %}


#### Kibana

DC/OS Kibana does not currently work with a DC/OS {{ data.techName }} configured with Transport Encryption enabled. It also does not currently support Transport Encryption for its own clients.
