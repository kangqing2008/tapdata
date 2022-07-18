package io.tapdata.common;

import com.tapdata.entity.Event;
import com.tapdata.entity.Setting;
import com.tapdata.entity.TapLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.simplejavamail.email.Email;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.email.EmailPopulatingBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xj
 * 2020-03-04 18:55
 **/
public class DDLConfirmEmailEventExecutor extends BaseEmailExecutor implements EventExecutor {

	private Logger logger = LogManager.getLogger(getClass());

	private final String AUTO_DDL_CONFIRM = "TAPDATA DDL CONFIRM";

	public DDLConfirmEmailEventExecutor(SettingService settingService) {
		super(settingService, null);
	}

	@Override
	public Event execute(Event event) {
		try {
			if (mailer == null || checkSettings()) {
				synchronized (this) {
					if (mailer == null || checkSettings()) {
						mailer = initMailer();
					}
				}
			}
			if (mailer != null) {

				Setting userSett = settingService.getSetting("smtp.server.user");
				String titlePrefix = settingService.getString("email.title.prefix");
				Setting sendAddress = settingService.getSetting("email.send.address");
				String fromAddress = sendAddress != null && StringUtils.isNotBlank(sendAddress.getValue()) ? sendAddress.getValue() : userSett.getValue();

				Map<String, Object> eventData = event.getEvent_data();
				String title = (String) eventData.getOrDefault("title", AUTO_DDL_CONFIRM);
				if (StringUtils.isNotBlank(titlePrefix)) {
					title = titlePrefix + title;
				}
				EmailPopulatingBuilder emailPopulatingBuilder = EmailBuilder.startingBlank();
				emailPopulatingBuilder
						.from("", fromAddress)
						.withSubject(title)
						.withHTMLText(assemblyMessageBody((String) eventData.getOrDefault("message", "")));

				Setting receiverSetting = settingService.getSetting("email.receivers");

				if (receiverSetting != null) {

					//receivers包含多个接收人，依次添加
					String receivers = receiverSetting.getValue();
					for (String receiver : receivers.split(",")) {
						emailPopulatingBuilder.to("", receiver);
					}
					Email email = emailPopulatingBuilder.buildEmail();

					//发送邮件
					mailer.sendMail(email);

				}

				event.setConsume_ts(System.currentTimeMillis());
				event.setEvent_status(Event.EVENT_STATUS_SUCCESSED);

			} else {
				Map<String, Object> failedResult = event.getFailed_result();
				if (failedResult == null) {
					failedResult = new HashMap<>();
					event.setFailed_result(failedResult);
				}
				failedResult.put("fail_message", "Please setting SMTP Server config.");
				failedResult.put("ts", System.currentTimeMillis());
				event.setEvent_status(Event.EVENT_STATUS_FAILED);
			}
			logger.info(TapLog.JOB_LOG_0005.getMsg(), event.getName(), event.getEvent_status());

		} catch (Exception e) {
			logger.error(TapLog.ERROR_0001.getMsg(), e.getMessage(), e);
			Map<String, Object> failedResult = event.getFailed_result();
			if (failedResult == null) {
				failedResult = new HashMap<>();
				event.setFailed_result(failedResult);
			}
			String message = e.getMessage();
			failedResult.put("fail_message", message);
			failedResult.put("ts", System.currentTimeMillis());
			event.setEvent_status(Event.EVENT_STATUS_FAILED);
		}

		return event;
	}
}