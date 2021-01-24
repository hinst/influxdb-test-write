import fetch from 'node-fetch';
import fs from 'fs';
import influxdb_client_apis from '@influxdata/influxdb-client-apis';
import { DateTime, Duration } from 'luxon';
import lodash from 'lodash';
import { sleep } from './sleep';

class App {
    apiUrl = 'http://localhost:8086/api/v2';
    orgId = '9c7a0f5daecd9269';
    apiToken = fs.readFileSync('./influxDbToken.txt').toString().trim();
    authorizationHeader = { Authorization: 'Token ' + this.apiToken };

    async run() {
        await this.deleteTestBucket();
        await sleep(100);
        const testBucket = await this.createTestBucket();
        await this.writeTestData();
    }

    private async createTestBucket(): Promise<influxdb_client_apis.Buckets> {
        const createBucketResponse = await fetch(this.apiUrl + '/buckets', {
            headers: this.authorizationHeader,
            method: 'POST',
            body: JSON.stringify({
                orgID: this.orgId,
                name: 'test',
                retentionRules: []
            }),
        });
        return await createBucketResponse.json();
    }

    private async deleteTestBucket() {
        const bucketList = await this.getBucketList();
        const testBucket = bucketList.buckets.find(b => b.name == 'test');
        if (testBucket != null) {
            await this.deleteBucket(testBucket.id);
        }
    }

    private async deleteBucket(id: string) {
        const deleteBucketResponse = await fetch(this.apiUrl + '/buckets/' + encodeURIComponent(id), {
            headers: this.authorizationHeader,
            method: 'DELETE',
        });
        return deleteBucketResponse.ok;
    }

    private async getBucketList(): Promise<influxdb_client_apis.Buckets> {
        const listBucketsResponse = await fetch(this.apiUrl + '/buckets', {
            method: 'GET',
            headers: this.authorizationHeader
        });
        if (!listBucketsResponse.ok)
            throw new Error(await listBucketsResponse.text());
        return await listBucketsResponse.json();
    }

    private async writeTestData() {
        const values = App.generateTestValues();
        console.log('Writing test data; count of values: ' + values.length);
        const valueGroups = lodash.chunk(values, 1000);
        for (const valueGroup of valueGroups) {
            const text = App.getLineProtocolText(valueGroup);
            if (false) console.log(DateTime.fromMillis(valueGroup[0][0]).setZone('UTC').toString());
            const response = await fetch(this.apiUrl + '/write?org=QBRX&bucket=test', {
                headers: this.authorizationHeader,
                method: 'POST',
                body: text
            });
            if (!response.ok)
                throw new Error('Cannot write values: ' + response.statusText);
        }
    }

    private static generateTestValues(): [number, number][] {
        const startDateTime = DateTime.fromISO('2020-01-01T00:00Z');
        const endDateTime = DateTime.fromISO('2020-03-01T00:00Z').minus({milliseconds: 1});
        const timedValues: [number, number][] = [];
        for (let currentDate = startDateTime, currentValue = 0;
            currentDate < endDateTime;
            currentDate = currentDate.plus({minutes: 1}), ++currentValue
        ) {
            timedValues.push([currentDate.toMillis(), currentValue]);
        }
        return timedValues;
    }

    private static getLineProtocolText(timedValues: [number, number][]) {
        return timedValues
            .map(timedValue => 'testMeasurement value=' + timedValue[1] + ' ' + timedValue[0] * 1000_000)
            .join('\n');
    }
}

new App().run();