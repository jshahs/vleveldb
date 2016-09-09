%%MQTT database

%%Subscription data

-record(vmq_subscription,{subscriberId,
			topic }).

-record(vmq_offline_store,{subscriberId,
                        vmq_msg }).


