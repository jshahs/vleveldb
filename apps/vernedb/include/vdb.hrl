%%MQTT database

%%Subscription data

-record(vdb_topics,{topic,
		   subscriberId }).

-record(vdb_offline_store,{subscriberId,
                        vmq_msg }).


-record(vdb_users,{subscriberId,
			 status,
                         on_node,
			sessionId }).
