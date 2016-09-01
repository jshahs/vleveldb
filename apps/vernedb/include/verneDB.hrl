%%MQTT database

%%Subscription data

-record(vmq_trie_subs,{subscriberId,
			topic }).

-record(vmq_offline_store,{subscriberId,
                        vmq_msg }).


