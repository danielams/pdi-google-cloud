# easydev

The concept of the feature-easydev branch is to show a concept of making steps quicker to implement.

I've taken the BigQuery step in the org.itfactory.kettle.steps.bigquerystream package and refactored it to use
utility base classes, making the custom step easier to code. 

The refactored implementation has 67% less code, and can be found in the bigquerystreameasy package.

The base classes can be found in the org.itfactory.kettle package. The classes are:-
- BaseHelperStep - breaks out the init and processRow functions in to handleInit, beforeFirstRow, handleRow, and afterLastRow functions, and provides default implementations for other methods
- BaseHelperStepMeta - 95% implementation of a Meta class. Remainder is for subclass to imlplement due to @Step annotation and I18N package configuration
- BaseHelperStepDialog - 95% implementation of the UI code, using configuration rather than explicit SWT code or XUL
- GenericDialogParameter - Provides a basic definition for meta field configuration, used to draw the UI and populate the configuration hashtable in BaseHelperStepMeta

