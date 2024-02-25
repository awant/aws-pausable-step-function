import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import * as PausableStepFunction from '../lib/pausable-step-function-stack';


test('Pausable step function created', () => {
  const app = new cdk.App();
  const stack = new PausableStepFunction.PausableStepFunctionStack(app, 'PausableStepFunctionStack');
  const template = Template.fromStack(stack);
    template.hasResource('AWS::StepFunctions::StateMachine', {
      Properties: {
        StateMachineName: 'PausableStepFunction',
        StateMachineType: 'STANDARD',
      },
    });
});
