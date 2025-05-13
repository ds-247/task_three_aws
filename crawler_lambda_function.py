def lambda_handler(event, context):
    print("Received event:", event)

    detail = event.get("detail", {})
    job_name = detail.get("jobName")
    state = detail.get("state")

    if state == "SUCCEEDED":
        print(f"Glue job '{job_name}' succeeded.")
        # handle success
    elif state == "FAILED":
        print(f"Glue job '{job_name}' failed.")
        # handle failure
    else:
        print(f"Glue job '{job_name}' ended with state: {state}")
        # handle other states like TIMEOUT, STOPPED, etc.
