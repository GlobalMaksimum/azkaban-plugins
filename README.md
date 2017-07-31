# azkaban-plugins

This repository contains azkaban-plugins developed by Global Maksimum.

Currently, there are two plugins whose documentations can be found under related folders. 

* [runsql](https://github.com/GlobalMaksimum/azkaban-plugins/tree/master/runsql)
* [copytable](https://github.com/GlobalMaksimum/azkaban-plugins/tree/master/copytable)*(not completed)*

## How to Install ?

Before starting Azkaban,

* Place released version of the project under `extlib` folder.
* Create related folders under `plugins/jobtypes`.
* Create **private.property** file for each job type.

In this case there should be 
* `plugins/jobtypes/runsql`
* `plugins/jobtypes/copytable` 

directories which will contain property files of **runsql** and **copytable** job types, respectively.

After all, Azkaban directory should look like this:

```
azkaban
    ...
    extlib
        ...
        AzkabanJobs.jar
    plugins
        ...
        jobtypes
            ...
            runsql
                private.properties
            copytable
                private.properties
```


