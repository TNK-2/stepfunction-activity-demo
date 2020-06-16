package com.example.stepfunctionsdemo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClient;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskRequest;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskResult;
import com.amazonaws.services.stepfunctions.model.SendTaskFailureRequest;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class StepfunctionsDemoApplication {

	private final String ACCESS_KEY = "";
	private final String SECRET_KEY = "";
	private final String ACTIVITY_ARN = "";
	private final String ACTIVITY_ARN2 = "";

	public static void main(String[] args) {
		try (ConfigurableApplicationContext ctx = SpringApplication.run(StepfunctionsDemoApplication.class, args)) {
			StepfunctionsDemoApplication app = ctx.getBean(StepfunctionsDemoApplication.class);
			app.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run(String... args) throws Exception {
		System.out.println("Worker1 start.");
		AWSStepFunctionsClient client = new AWSStepFunctionsClient(
				new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY),
				new ClientConfiguration());
		Region region = Region.getRegion(Regions.AP_NORTHEAST_1);
		client.setRegion(region);

		String greetingResult;
		boolean flag1 = true;
		while (flag1) {
			System.out.println("Worker1 get activities.");
			GetActivityTaskResult getActivityTaskResult = client.getActivityTask(new GetActivityTaskRequest().withActivityArn(ACTIVITY_ARN));
			System.out.println("Worker1 get activities complete.");

			if (getActivityTaskResult != null) {
				try {
					JsonNode json = Jackson.jsonNodeOf(getActivityTaskResult.getInput());
					greetingResult = getGreeting(json.get("who").textValue());
					client.sendTaskSuccess(new SendTaskSuccessRequest().withOutput(greetingResult)
							.withTaskToken(getActivityTaskResult.getTaskToken()));
					flag1 = false;
					System.out.println("Worker sendTaskSuccess.");
				} catch (Exception e) {
					e.printStackTrace();
					client.sendTaskFailure(new SendTaskFailureRequest().withTaskToken(getActivityTaskResult.getTaskToken()));
					flag1 = false;
				}
			} else {
				Thread.sleep(5000);
			}
		}

		Thread.sleep(10000);

		System.out.println("Worker2 start.");
		AWSStepFunctionsClient client2 = new AWSStepFunctionsClient(
				new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY),
				new ClientConfiguration());
		client2.setRegion(region);
		String greetingResult2;
		boolean flag2 = true;
		while (flag2) {
			System.out.println("Worker2 get activities.");
			GetActivityTaskResult getActivityTaskResult2 = client2.getActivityTask(new GetActivityTaskRequest().withActivityArn(ACTIVITY_ARN2));
			System.out.println("Worker2 get activities complete.");

			if (getActivityTaskResult2 != null) {
				try {
					JsonNode json = Jackson.jsonNodeOf(getActivityTaskResult2.getInput());
					greetingResult2 = getGreeting(json.get("Hello").textValue());
					client2.sendTaskSuccess(new SendTaskSuccessRequest().withOutput(greetingResult2)
							.withTaskToken(getActivityTaskResult2.getTaskToken()));
					flag2 = false;
					System.out.println("Worker2 sendTaskSuccess.");
				} catch (Exception e) {
					e.printStackTrace();
					client2.sendTaskFailure(new SendTaskFailureRequest().withTaskToken(getActivityTaskResult2.getTaskToken()));
					flag2 = false;
				}
			} else {
				Thread.sleep(5000);
			}
		}
	}

	private String getGreeting(String who) throws Exception {
		return "{\"Hello\": \"" + who + "\"}";
	}
}
