package com.es.offline;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import com.es.offline.util.Pattern;

public class EsAdminUtil {
	
	private static final Logger _LOG = Logger.getLogger(EsAdminUtil.class);


	public static void updateDynamicSettings() throws Exception {
		Client client=ESClientUtil.getClient();

		Settings settings = ImmutableSettings.settingsBuilder().put("index.refresh_interval", "5s").build();

		UpdateSettingsRequest request = new UpdateSettingsRequest();
		request.settings(settings);

		UpdateSettingsResponse res = client.admin().indices().execute(UpdateSettingsAction.INSTANCE, request)
				.actionGet();

		System.out.println(res.isAcknowledged());
		
		
		GetSettingsResponse gres= client.admin().indices().prepareGetSettings("ddos*").execute().actionGet();
		
		System.out.println(gres.getIndexToSettings().get("ddos-2014-bb").toDelimitedString(','));
		
		client.close();

	}
	
	public static String[]  queryIndexNames(Client client, String[] prefixs) {
		IndicesStatsResponse stats = client.admin().indices().prepareStats().execute().actionGet();

		Set<String> set = stats.getIndices().keySet();

		Map<String, String> template_map = Maps.newHashMap();
		Set<String> indices = Sets.newHashSet();
		Set<String> snapshotIndices = Sets.newHashSet();

		_LOG.info(Arrays.toString(prefixs));

		for (String str : prefixs) {

			if (str.indexOf("[") != -1 && str.indexOf("]") != -1) {
				String template = StringUtils.substringBetween(str, "[", "]");

				String service = str.substring(0, str.indexOf("["));
				if (service.endsWith("-")) {
					service = StringUtils.removeEnd(service, "-");
				}

				template_map.put(service, template);
			} else if (StringUtils.endsWith(str, "*")) {

				for (String index : set) {
					if (index.startsWith(StringUtils.removeEnd(str, "*"))) {
						indices.add(index);
					}
				}
			} else {

				indices.add(str); // 配置的索引名

			}
		}

		if (template_map.size() > 0) {

			for (String index1 : set) {

				String service1 = Pattern.parsing(index1);

				String template1 = template_map.get(service1);

				if (template1 != null) {
					String templ = Pattern.matchPatternForIndex(index1);
					if (templ != null && templ.equals(template1)) {
						snapshotIndices.add(index1);
					}
				}

			}

		}

		snapshotIndices.addAll(indices);

		_LOG.info(snapshotIndices);
		
		String[] arr=snapshotIndices.toArray(new String[snapshotIndices.size()]);
		
		return arr;


	}
	
}
