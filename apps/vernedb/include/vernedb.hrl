%%MQTT database

%%Subscription data

-record(vmq_subscriber,{subscriberId,
			topic }).

-record(vmq_offline_store,{subscriberId,
                        vmq_msg }).


