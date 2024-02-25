import {
  aws_stepfunctions as sfn,
  Stack,
  StackProps,
  aws_sns as sns,
  aws_stepfunctions_tasks as tasks,
  aws_lambda as lambda,
  Duration,
  aws_lambda_event_sources as lambda_event_sources,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export class PausableStepFunctionStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const callbackTopic = new sns.Topic(this, 'CallbackToUnpauseSfnTopic', {
      topicName: 'CallbackTopicToUnpauseSfn',
    });

    const executionWorkflow = new sfn.Pass(this, 'Preprocessing')
        .next(this.buildCallAsyncServiceStep('Long job execution', callbackTopic))
        .next(new sfn.Pass(this, 'Post processing'));

    const workflow = new sfn.StateMachine(this, 'PausableStepFunction', {
      stateMachineName: 'PausableStepFunction',
      definitionBody: sfn.DefinitionBody.fromChainable(executionWorkflow),
      stateMachineType: sfn.StateMachineType.STANDARD,
    });
    this.buildResumeStepFunctionLambda(workflow, callbackTopic);
  }

  private buildCallAsyncServiceStep(stepName: string, callbackTopic: sns.Topic): tasks.LambdaInvoke {
    const callAsyncServiceLambda = this.buildLambdaFunction('CallAsyncServiceLambda', 'call_async_service', {
      CALLBACK_TOPIC: callbackTopic.topicArn,
    });
    callbackTopic.grantPublish(callAsyncServiceLambda);

    return new tasks.LambdaInvoke(this, stepName, {
      lambdaFunction: callAsyncServiceLambda,
      integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
      payload: sfn.TaskInput.fromObject({
        taskToken: sfn.JsonPath.taskToken,
        isManualCallback: sfn.JsonPath.stringAt('$.IsManualCallback'),
      }),
      resultPath: '$.CallAsyncServiceStep',
    });
  }

  private buildResumeStepFunctionLambda(workflow: sfn.StateMachine, callbackTopic: sns.ITopic): lambda.Function {
    const resumeStepFunctionLambda = this.buildLambdaFunction('ResumeStepFunctionLambda', 'resume_sfn');
    resumeStepFunctionLambda.addEventSource(new lambda_event_sources.SnsEventSource(callbackTopic));
    workflow.grantTaskResponse(resumeStepFunctionLambda);
    return resumeStepFunctionLambda;
  }

  private buildLambdaFunction(
      functionName: string,
      filename: string,
      environment?: {
        [key: string]: string;
      },
  ): lambda.Function {
    return new lambda.Function(this, functionName, {
      functionName,
      timeout: Duration.seconds(60),
      memorySize: 128,
      code: lambda.Code.fromAsset('assets'),
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: `app.${filename}.handler`,
      environment,
    });
  }
}
