# README for D365 Record Deletion Console App

## Overview

The D365 Record Deletion Console App is a C# application designed to help you delete records from a Dataverse environment. This app provides a flexible and efficient way to delete large quantities of records while allowing for configuration of batch size and retry options.
The application retrieves all records to be deleted from Dataverse then splits the records across multiple application accounts defined in the config.
The application also creates multiple threads per each application account to maximise the throuput 

## Requirements

- Microsoft Dynamics 365 (D365) environment.
- Access to the D365 API.
- Application account IDs and Secrets with permissions to delete records.
- Visual Studio or a compatible C# development environment.
- .NET Framework or .NET Core, depending on your development environment.

## Getting Started

1. Clone or download this repository to your local development environment.
2. Open the solution in Visual Studio or your preferred C# development environment.
3. Build the solution to ensure all dependencies are resolved.

## Configuration

Before running the application, you need to configure the app by providing the necessary parameters:

1. **Entity Name**: Enter the name of the D365 entity from which you want to delete records.

2. **Batch Size**: Specify the number of records to process in each batch. If you don't provide a valid value, the default batch size is 100.

3. **Client Configuration**: Define the client configuration details in the app.config file. You can specify multiple clients with their respective ClientId and ClientSecret for parallel processing. This needs to be in KVP JSON format (e.g. [{"clientid";:"<ClientID1>,"clientsecret":"<ClientSecret1>"},{"clientid";:"<ClientID2>,"clientsecret":"<ClientSecret2>"},....]) 

4. **D365 URL**: Set the URL of your D365 environment in the app.config file.

5. **Processing Retries**: Configure the number of processing retries in the app.config file. This allows you to retry the deletion process if it fails or times out.

6. **Thread Count**: Adjust the number of threads for parallel processing in the app.config file.

## Usage

1. Run the application.

2. Enter the entity name when prompted.

3. Enter the batch size when prompted, or press Enter to use the default value.

4. The application will start deleting records from the specified entity based on the configured batch size and retry options.

5. You will see progress and status updates as the records are deleted.

6. Once the deletion process is complete, you can press Enter to exit the application.

## Monitoring Progress

The application provides real-time progress updates, including the number of records processed, percentage completion, estimated remaining time, and transfer rate. This information is displayed in the console window.

## Handling Errors

The application includes error handling to address potential issues during the deletion process. Any errors encountered will be logged, and the app will continue processing the remaining records.

## License

This project is provided under an open-source license. You can find the license details in the LICENSE file included in the repository.

## Support

For questions, bug reports, or feature requests, please create an issue in the project's GitHub repository.

## Disclaimer

This application is intended for use in non-production environments for testing and development purposes. Always exercise caution when working with production data in a D365 environment.

---

Feel free to customize this README to provide additional details, such as contact information or specific usage instructions for your team.
